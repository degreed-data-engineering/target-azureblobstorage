import os
import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient
import re
import logging
import pyarrow
import pyarrow.parquet as pq
import json
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
import uuid
from decimal import Decimal, InvalidOperation
from typing import Optional, Union, Dict, Any # Added Optional and Union, Dict, Any for type hinting

logger = logging.getLogger(__name__)

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming with batching."""

    DEFAULT_BATCH_SIZE_ROWS = 10000
    DEFAULT_DECIMAL_PRECISION = 38
    DEFAULT_DECIMAL_SCALE = 9

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.container_client: Optional[Any] = None # Using Any for BlobContainerClient due to potential import cycle with azure.storage.blob itself
        self.local_file_path_template: Optional[str] = None
        self.blob_path_template: Optional[str] = None
        self.stream_initialized: bool = False
        self.output_format: str = "csv"
        self.records_buffer: list = []
        self.batch_file_counter: int = 0
        self._pyarrow_schema: Optional[pyarrow.Schema] = None

    @property
    def batch_size_rows(self) -> int:
        return int(self.config.get("batch_size_rows", self.DEFAULT_BATCH_SIZE_ROWS))

    def get_output_format_from_filename(self, filename: str) -> str:
        if filename.lower().endswith(".parquet"):
            return "parquet"
        elif filename.lower().endswith(".jsonl") or filename.lower().endswith(".json"):
            return "jsonl"
        return "csv"

    def _generate_batch_suffix(self) -> str:
        return f"part_{self.batch_file_counter:04d}_{uuid.uuid4().hex[:8]}"

    def _get_batch_blob_path(self) -> str:
        if self.output_format == "csv":
            return self.blob_path_template or "" # Should be set in start_stream
        
        if not self.blob_path_template: # Should not happen if stream_initialized
             self.logger.error("blob_path_template is not set in _get_batch_blob_path")
             # Fallback or raise error
             return f"{self.stream_name}/error_unknown_path_{self._generate_batch_suffix()}.{self.output_format}"

        base_path, extension = os.path.splitext(self.blob_path_template)
        batch_suffix = self._generate_batch_suffix()
        return f"{base_path}_{batch_suffix}{extension}"

    def _get_batch_local_file_path(self, batch_blob_path: str) -> str:
        stream_tmp_dir = os.path.join("/tmp", f"target_azure_{self.stream_name}")
        os.makedirs(stream_tmp_dir, exist_ok=True)
        return os.path.join(stream_tmp_dir, os.path.basename(batch_blob_path))

    def _singer_type_to_pyarrow_type(self, singer_type_def: Dict[str, Any], property_name: str) -> Optional[pyarrow.DataType]: # Corrected type hint
        """Maps a single Singer type definition to a PyArrow type."""
        singer_types = singer_type_def.get("type", ["null", "string"])
        
        if "number" in singer_types:
            if self.stream_name == "public-Licenses" and property_name == "InternalCost":
                # IMPORTANT: Verify and set correct precision and scale for InternalCost
                return pyarrow.decimal128(precision=18, scale=4) # EXAMPLE - ADJUST THIS!
            # Check for logical type "decimal" if available in format for other number types
            # logical_type = singer_type_def.get("logicalType") # Singer spec for logical types
            # format_hint = singer_type_def.get("format") # another place for hints
            # For now, using a general decimal for all "number" types if not InternalCost
            return pyarrow.decimal128(self.DEFAULT_DECIMAL_PRECISION, self.DEFAULT_DECIMAL_SCALE)
        elif "integer" in singer_types:
            return pyarrow.int64()
        elif "boolean" in singer_types:
            return pyarrow.bool_()
        elif "string" in singer_types:
            fmt = singer_type_def.get("format")
            if fmt == "date-time":
                return pyarrow.timestamp('us', tz='UTC')
            elif fmt == "date":
                return pyarrow.date32()
            return pyarrow.string()
        elif "array" in singer_types:
            item_type_def = singer_type_def.get("items", {})
            pa_item_type = self._singer_type_to_pyarrow_type(item_type_def, f"{property_name}_items")
            if pa_item_type:
                return pyarrow.list_(pa_item_type)
            self.logger.warning(f"Could not determine item type for array '{property_name}'. Defaulting array items to string.")
            return pyarrow.list_(pyarrow.string())
        elif "object" in singer_types:
            return pyarrow.string() # Store nested objects as JSON strings

        self.logger.warning(f"Unknown Singer type(s) {singer_types} for property '{property_name}'. Defaulting to string.")
        return pyarrow.string()

    def _build_pyarrow_schema(self) -> Optional[pyarrow.Schema]:
        if self._pyarrow_schema:
            return self._pyarrow_schema

        if not self.schema: # self.schema is from the SDK, based on SCHEMA messages
            self.logger.error(f"Singer schema (self.schema) not available for stream {self.stream_name}.")
            return None

        fields = []
        for property_name, property_definition in self.schema.get("properties", {}).items():
            pa_type = self._singer_type_to_pyarrow_type(property_definition, property_name)
            if pa_type:
                # Check for null in the 'type' array for nullability
                is_nullable = "null" in property_definition.get("type", [])
                fields.append(pyarrow.field(property_name, pa_type, nullable=is_nullable))
        
        if not fields:
            self.logger.warning(f"No fields derived for PyArrow schema for stream {self.stream_name}.")
            return None
        
        self._pyarrow_schema = pyarrow.schema(fields)
        self.logger.info(f"Built PyArrow schema for {self.stream_name}: {self._pyarrow_schema}")
        return self._pyarrow_schema

    def start_stream(self, context: Dict[str, Any]) -> None:
        self.logger.info(f"Starting stream for {self.stream_name}")
        account_name = self.config["storage_account_name"]
        account_key = self.config["storage_account_key"]
        container_name = self.config.get("container_name", "default-container")
        connection_string = self.config.get(
            "azure_storage_connection_string",
            f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        )

        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.blob_service_client.get_container_client(container_name)

        try:
            self.container_client.create_container()
            self.logger.info(f"Created container: {container_name}")
        except ResourceExistsError:
            self.logger.info(f"Container {container_name} already exists.")
        except Exception as e:
            self.logger.error(f"Failed to create or access container {container_name}: {e}", exc_info=True)
            raise

        self.blob_path_template = self.format_file_name()
        self.output_format = self.get_output_format_from_filename(self.blob_path_template)

        if self.output_format == "parquet":
            self._pyarrow_schema = self._build_pyarrow_schema()
            if not self._pyarrow_schema:
                msg = f"Failed to build PyArrow schema for Parquet output on stream {self.stream_name}. Cannot proceed."
                self.logger.critical(msg) # Use critical for unrecoverable errors
                raise ValueError(msg)

        if self.output_format == "csv":
            self.local_file_path_template = os.path.join("/tmp", os.path.basename(self.blob_path_template))
            os.makedirs(os.path.dirname(self.local_file_path_template), exist_ok=True)
            if not os.path.exists(self.local_file_path_template):
                with open(self.local_file_path_template, 'w', encoding='utf-8') as f:
                    f.write('')
        
        self.logger.debug(f"Stream {self.stream_name} initialized. Output format: {self.output_format.upper()}. Batch size: {self.batch_size_rows} rows.")
        self.stream_initialized = True
        self.records_buffer = []
        self.batch_file_counter = 0

    def _coerce_record_to_schema(self, record: Dict[str, Any], pa_schema: pyarrow.Schema) -> Dict[str, Any]:
        coerced_record: Dict[str, Any] = {} # Ensure type for coerced_record
        for field in pa_schema:
            col_name = field.name
            pa_type = field.type
            raw_value = record.get(col_name)

            if raw_value is None:
                coerced_record[col_name] = None
                continue

            try:
                if pyarrow.is_decimal_type(pa_type):
                    # PyArrow expects Python Decimal for decimal128/256 from objects
                    coerced_record[col_name] = Decimal(str(raw_value))
                elif pyarrow.is_timestamp_type(pa_type):
                    # pd.to_datetime is robust
                    dt_val = pd.to_datetime(raw_value, errors='coerce', utc=False) # Parse without forcing UTC yet
                    if pd.isna(dt_val):
                        self.logger.warning(f"Could not parse timestamp '{raw_value}' for column '{col_name}'. Setting to None.")
                        coerced_record[col_name] = None
                    else:
                        # If pa_type.tz is set (e.g. 'UTC'), localize or convert
                        if pa_type.tz:
                             if dt_val.tzinfo is None:
                                 coerced_record[col_name] = dt_val.tz_localize(pa_type.tz)
                             else:
                                 coerced_record[col_name] = dt_val.tz_convert(pa_type.tz)
                        else: # no timezone in pyarrow type
                             coerced_record[col_name] = dt_val.tz_localize(None) if dt_val.tzinfo else dt_val


                elif pyarrow.is_date_type(pa_type):
                    dt_obj = pd.to_datetime(raw_value, errors='coerce')
                    coerced_record[col_name] = dt_obj.date() if pd.notna(dt_obj) else None
                    if coerced_record[col_name] is None and raw_value is not None:
                         self.logger.warning(f"Could not parse date '{raw_value}' for column '{col_name}'. Setting to None.")
                elif pyarrow.is_integer_type(pa_type):
                    coerced_record[col_name] = int(float(str(raw_value))) # str() first, then float, then int
                elif pyarrow.is_floating_type(pa_type):
                    coerced_record[col_name] = float(str(raw_value))
                elif pyarrow.is_boolean_type(pa_type):
                    if isinstance(raw_value, str):
                        if raw_value.lower() in ("true", "t", "1", "yes", "y"): coerced_record[col_name] = True
                        elif raw_value.lower() in ("false", "f", "0", "no", "n"): coerced_record[col_name] = False
                        else: 
                            self.logger.warning(f"Could not parse boolean string '{raw_value}' for column '{col_name}'. Setting to None.")
                            coerced_record[col_name] = None
                    else:
                        coerced_record[col_name] = bool(raw_value)
                elif pyarrow.is_string_type(pa_type) and not isinstance(raw_value, str):
                    if isinstance(raw_value, (dict, list)):
                        coerced_record[col_name] = json.dumps(raw_value)
                    else:
                        coerced_record[col_name] = str(raw_value)
                else: # Already string or other compatible type
                    coerced_record[col_name] = raw_value
            except (ValueError, TypeError, InvalidOperation, OverflowError) as e: # Added OverflowError
                self.logger.warning(
                    f"Type coercion error for column '{col_name}' with value '{raw_value}' "
                    f"(expected PyArrow type {pa_type}): {e}. Setting to None."
                )
                coerced_record[col_name] = None
        
        # Handle extra fields not in schema by attempting to stringify them
        for col_name, raw_value in record.items():
            if col_name not in coerced_record: # If not processed by schema fields
                self.logger.debug(f"Column '{col_name}' found in record but not in PyArrow schema. Adding as best-effort string.")
                if isinstance(raw_value, (dict,list)):
                    try:
                        coerced_record[col_name] = json.dumps(raw_value)
                    except TypeError:
                        coerced_record[col_name] = str(raw_value) # Fallback
                elif raw_value is not None:
                    coerced_record[col_name] = str(raw_value)
                else:
                    coerced_record[col_name] = None

        return coerced_record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        if not self.stream_initialized:
            self.logger.warning("Stream not initialized in process_record, attempting to initialize.")
            self.start_stream(context)

        if self.output_format == "parquet" and self._pyarrow_schema:
            processed_record = self._coerce_record_to_schema(record, self._pyarrow_schema)
            self.records_buffer.append(processed_record)
        else: # For CSV or if Parquet schema build failed (should have raised in start_stream)
            self.records_buffer.append(record)

        if len(self.records_buffer) >= self.batch_size_rows:
            self.logger.info(f"Buffer for {self.stream_name} ({self.output_format}) reached {len(self.records_buffer)} records. Draining batch.")
            self._drain_batch()

    def _drain_batch(self) -> None:
        if not self.records_buffer:
            self.logger.debug(f"No records in buffer for {self.stream_name}, skipping batch drain.")
            return

        current_batch_blob_path = self._get_batch_blob_path()
        current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
        
        self.logger.info(f"Draining {len(self.records_buffer)} records for {self.stream_name} to local file {current_local_batch_file} for blob {current_batch_blob_path}")

        try:
            # Create DataFrame from (potentially coerced) records
            # It's crucial that _coerce_record_to_schema prepares data appropriately for DataFrame creation
            df = pd.DataFrame(self.records_buffer)

            if self.output_format == "parquet":
                if not self._pyarrow_schema: # Should have been set in start_stream
                    self.logger.error(f"PyArrow schema is None in _drain_batch for Parquet on stream {self.stream_name}. This indicates an earlier failure.")
                    raise ValueError(f"Cannot write Parquet for {self.stream_name} without a PyArrow schema.")

                # Ensure DataFrame columns align with PyArrow schema (order and existence)
                # Create a new DataFrame with columns in schema order, adding missing ones as Nones
                data_for_arrow = {}
                for field in self._pyarrow_schema:
                    if field.name in df.columns:
                        data_for_arrow[field.name] = df[field.name]
                    else:
                        # Create a pandas Series of Nones with an object dtype if column is missing.
                        # PyArrow will handle None based on the field's nullability in the schema.
                        data_for_arrow[field.name] = pd.Series([None] * len(df), dtype='object', name=field.name)
                
                df_aligned = pd.DataFrame(data_for_arrow, columns=[field.name for field in self._pyarrow_schema])
                
                arrow_table = pyarrow.Table.from_pandas(df_aligned, schema=self._pyarrow_schema, safe=True)
                with open(current_local_batch_file, 'wb') as f:
                    pq.write_table(arrow_table, f)

            elif self.output_format == "jsonl":
                with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                    for rec in self.records_buffer:
                        f.write(json.dumps(self._serialize_json_safe(rec)) + '\n')
            
            elif self.output_format == "csv":
                # Current design defers CSV writing to clean_up for a single file.
                # If _drain_batch is called for CSV, it means batch_size_rows was hit.
                # To handle this, we'd need to write a partial CSV.
                # For this fix, CSV writes the whole buffer (or part) if called.
                self.logger.info(f"Writing {len(self.records_buffer)} CSV records to {current_local_batch_file} (batch).")
                # Header logic for batched CSVs: write header only for the very first batch file (part_0000)
                is_first_batch_file = self.batch_file_counter == 0
                df.to_csv(current_local_batch_file, index=False, header=is_first_batch_file, encoding='utf-8', lineterminator='\n')


            file_size = os.path.getsize(current_local_batch_file)
            self.logger.info(f"Successfully wrote batch to {current_local_batch_file}. Size: {file_size} bytes. Uploading to {current_batch_blob_path}.")

            blob_client_for_batch = self.container_client.get_blob_client(blob=current_batch_blob_path)
            with open(current_local_batch_file, "rb") as data:
                blob_client_for_batch.upload_blob(data, overwrite=True)
            self.logger.info(f"Successfully uploaded batch file {current_batch_blob_path}")

        except Exception as e:
            self.logger.error(f"Failed to process or upload batch for {self.stream_name} to {current_batch_blob_path}: {e}", exc_info=True)
            raise
        finally:
            if os.path.exists(current_local_batch_file):
                try:
                    os.remove(current_local_batch_file)
                    self.logger.debug(f"Removed local batch file: {current_local_batch_file}")
                except Exception as e_rem:
                    self.logger.error(f"Error removing local batch file {current_local_batch_file}: {e_rem}")
            
            self.records_buffer = [] # Clear buffer after processing batch
            self.batch_file_counter += 1


    def _serialize_json_safe(self, record: Dict[str, Any]) -> Dict[str, Any]:
        safe_record: Dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                safe_record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                safe_record[key] = float(value) 
            elif pd.isna(value): # Handles pd.NaT, np.nan, etc.
                safe_record[key] = None
            elif isinstance(value, (float)) and (value != value): # Check for float NaN
                 safe_record[key] = None
            else:
                safe_record[key] = value
        return safe_record

    def format_file_name(self) -> str:
        naming_convention = self.config.get("naming_convention", "{stream}/{stream}_{timestamp}.csv") # Sensible default
        stream_name_safe = re.sub(r'[\\/*?:"<>|]', "_", self.stream_name)
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        
        file_name_pattern = naming_convention.replace("{stream}", stream_name_safe)
        # Only replace timestamp if the placeholder exists
        if "{timestamp}" in file_name_pattern:
            file_name_pattern = file_name_pattern.replace("{timestamp}", timestamp_str)
        
        # Sanitize each path component separately to preserve directory structure
        parts = file_name_pattern.split('/')
        sanitized_parts = [re.sub(r'[\\*?:"<>|]', "_", part) for part in parts]
        final_path = "/".join(sanitized_parts)
        
        self.logger.debug(f"Formatted base file name/path pattern: {final_path}")
        return final_path

    def clean_up(self) -> None:
        self.logger.info(f"Starting clean_up for stream {self.stream_name}. Records in buffer: {len(self.records_buffer)}")
        if not self.stream_initialized:
            self.logger.warning(f"Stream {self.stream_name} was not initialized. Skipping clean_up.")
            return

        if self.records_buffer: # If any records are left (last batch was smaller than batch_size_rows)
            self.logger.info(f"Draining remaining {len(self.records_buffer)} records for {self.stream_name} ({self.output_format}) during clean_up.")
            self._drain_batch() # This will write, upload, and clear the buffer. CSV will now also make a final batch file.
        
        # Logic for empty marker file if NO data was ever processed/batched
        # (i.e., records_buffer was always empty AND no batches were written)
        elif self.batch_file_counter == 0 and not self.records_buffer : 
            self.logger.info(f"No records processed and no batches written for {self.stream_name} ({self.output_format}). Writing an empty marker file.")
            current_batch_blob_path = self._get_batch_blob_path() # Gets part_0000...
            current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
            try:
                if self.output_format == "parquet":
                    if not self._pyarrow_schema: self._pyarrow_schema = self._build_pyarrow_schema() # Ensure schema is loaded
                    if self._pyarrow_schema:
                        empty_table = self._pyarrow_schema.empty_table()
                        with open(current_local_batch_file, 'wb') as f:
                            pq.write_table(empty_table, f)
                    else: # Fallback if schema couldn't be built (should have failed earlier)
                         self.logger.warning(f"Cannot write typed empty Parquet for {self.stream_name} due to missing schema. Writing 0-byte file.")
                         with open(current_local_batch_file, 'wb') as f: pass
                elif self.output_format == "jsonl":
                    with open(current_local_batch_file, 'w', encoding='utf-8') as f: pass # Empty file
                elif self.output_format == "csv":
                    with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                        # Write header for empty CSV if schema is available
                        if self.schema and "properties" in self.schema:
                            header_row = ",".join(self.schema["properties"].keys())
                            f.write(header_row + "\n")
                        # else, just an empty file
                
                if os.path.exists(current_local_batch_file):
                    blob_client_empty = self.container_client.get_blob_client(blob=current_batch_blob_path)
                    with open(current_local_batch_file, "rb") as data:
                        blob_client_empty.upload_blob(data, overwrite=True)
                    self.logger.info(f"Successfully uploaded empty marker file {current_batch_blob_path}")
            except Exception as e:
                self.logger.error(f"Failed to create or upload empty marker file for {self.stream_name}: {e}", exc_info=True)
            finally:
                if os.path.exists(current_local_batch_file):
                    try: os.remove(current_local_batch_file)
                    except: pass
        
        self.logger.info(f"Successfully cleaned up stream for {self.stream_name}")
        self.stream_initialized = False
        self.records_buffer = []
        self.batch_file_counter = 0
        self._pyarrow_schema = None