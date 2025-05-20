import os
import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient
import re
import logging
import pyarrow
import pyarrow.parquet as pq # For writing Parquet with explicit schema
import json
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
import uuid
from decimal import Decimal, InvalidOperation # For handling decimals

logger = logging.getLogger(__name__)

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming with batching."""

    DEFAULT_BATCH_SIZE_ROWS = 10000
    # Define default precision and scale for decimals if not otherwise specified.
    # These should be large enough for most use cases but can be overridden.
    DEFAULT_DECIMAL_PRECISION = 38
    DEFAULT_DECIMAL_SCALE = 9


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_service_client = None
        self.container_client = None
        self.local_file_path_template = None
        self.blob_path_template = None
        self.stream_initialized = False
        self.output_format = "csv"
        self.records_buffer = []
        self.batch_file_counter = 0
        self._pyarrow_schema = None # Cache for the stream's PyArrow schema

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
            return self.blob_path_template
        base_path, extension = os.path.splitext(self.blob_path_template)
        batch_suffix = self._generate_batch_suffix()
        return f"{base_path}_{batch_suffix}{extension}"

    def _get_batch_local_file_path(self, batch_blob_path: str) -> str:
        stream_tmp_dir = os.path.join("/tmp", f"target_azure_{self.stream_name}")
        os.makedirs(stream_tmp_dir, exist_ok=True)
        return os.path.join(stream_tmp_dir, os.path.basename(batch_blob_path))

    def _singer_type_to_pyarrow_type(self, singer_type_def: dict, property_name: str) -> pyarrow.DataType | None:
        """Maps a single Singer type definition to a PyArrow type."""
        singer_types = singer_type_def.get("type", ["null", "string"])
        # is_nullable = "null" in singer_types # Nullability handled by pyarrow.field

        # Prioritize specific types
        if "number" in singer_types:
            # Check for custom precision/scale in config or use defaults for specific fields
            # Example: self.config.get(f"{self.stream_name}_{property_name}_precision")
            # For simplicity, using a general decimal type here.
            # For "InternalCost" in "public-Licenses", let's assume it needs a specific decimal
            if self.stream_name == "public-Licenses" and property_name == "InternalCost":
                 # YOU MUST DETERMINE THIS FROM YOUR DATA for InternalCost
                 # e.g., if max is 99999.99 (7 digits total, 2 after decimal) -> precision=7, scale=2
                 # If max is 123.4567 (7 digits total, 4 after decimal) -> precision=7, scale=4
                 # Let's use a generous default that you should verify and adjust:
                return pyarrow.decimal128(precision=18, scale=4) # EXAMPLE - ADJUST THIS!
            else:
                # For other numbers, you could use float64 or a default decimal
                return pyarrow.decimal128(self.DEFAULT_DECIMAL_PRECISION, self.DEFAULT_DECIMAL_SCALE)
                # return pyarrow.float64() # Simpler but loses decimal exactness
        elif "integer" in singer_types:
            return pyarrow.int64()
        elif "boolean" in singer_types:
            return pyarrow.bool_()
        elif "string" in singer_types:
            fmt = singer_type_def.get("format")
            if fmt == "date-time":
                return pyarrow.timestamp('us', tz='UTC') # Store as UTC
            elif fmt == "date":
                return pyarrow.date32()
            # Add other string formats if needed (e.g., "time", "uuid")
            return pyarrow.string()
        elif "array" in singer_types:
            item_type_def = singer_type_def.get("items", {})
            pa_item_type = self._singer_type_to_pyarrow_type(item_type_def, f"{property_name}_items")
            if pa_item_type:
                return pyarrow.list_(pa_item_type)
            self.logger.warning(f"Could not determine item type for array '{property_name}'. Defaulting array items to string.")
            return pyarrow.list_(pyarrow.string()) # Fallback for array items
        elif "object" in singer_types:
            # For Parquet, complex objects are often stored as JSON strings
            # or flattened. For simplicity, let's serialize to JSON string.
            return pyarrow.string() # Store nested objects as JSON strings

        self.logger.warning(f"Unknown Singer type(s) {singer_types} for property '{property_name}'. Defaulting to string.")
        return pyarrow.string() # Fallback

    def _build_pyarrow_schema(self) -> pyarrow.Schema | None:
        """
        Builds a PyArrow schema from the Singer schema for the current stream.
        Caches the result in self._pyarrow_schema.
        """
        if self._pyarrow_schema:
            return self._pyarrow_schema

        if not self.schema:
            self.logger.error(f"Singer schema (self.schema) not available for stream {self.stream_name}.")
            return None

        fields = []
        for property_name, property_schema in self.schema.get("properties", {}).items():
            pa_type = self._singer_type_to_pyarrow_type(property_schema, property_name)
            if pa_type:
                is_nullable = "null" in property_schema.get("type", []) or not property_schema.get("required", False)
                fields.append(pyarrow.field(property_name, pa_type, nullable=is_nullable))
        
        if not fields:
            self.logger.warning(f"No fields derived for PyArrow schema for stream {self.stream_name}.")
            return None
        
        self._pyarrow_schema = pyarrow.schema(fields)
        self.logger.info(f"Built PyArrow schema for {self.stream_name}: {self._pyarrow_schema}")
        return self._pyarrow_schema

    def start_stream(self, context: dict) -> None:
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
            self._pyarrow_schema = self._build_pyarrow_schema() # Build schema at stream start
            if not self._pyarrow_schema:
                self.logger.error(f"Failed to build PyArrow schema for Parquet output on stream {self.stream_name}. Cannot proceed.")
                raise ValueError(f"Could not build PyArrow schema for {self.stream_name}")


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

    def _coerce_record_to_schema(self, record: dict, pa_schema: pyarrow.Schema) -> dict:
        """
        Coerces data types in a record to match the PyArrow schema.
        Handles decimals, dates, and potential type mismatches before DataFrame creation.
        """
        coerced_record = {}
        for field in pa_schema:
            col_name = field.name
            pa_type = field.type
            raw_value = record.get(col_name)

            if raw_value is None:
                coerced_record[col_name] = None
                continue

            try:
                if pyarrow.is_decimal_type(pa_type):
                    # Ensure it's a string first for consistent Decimal conversion
                    coerced_record[col_name] = Decimal(str(raw_value))
                elif pyarrow.is_timestamp_type(pa_type):
                    # Pandas to_datetime can handle various formats
                    coerced_record[col_name] = pd.to_datetime(raw_value, errors='coerce', utc=True)
                    if pd.isna(coerced_record[col_name]): # If conversion failed
                         self.logger.warning(f"Could not parse timestamp '{raw_value}' for column '{col_name}'. Setting to None.")
                         coerced_record[col_name] = None
                elif pyarrow.is_date_type(pa_type):
                    dt_obj = pd.to_datetime(raw_value, errors='coerce')
                    coerced_record[col_name] = dt_obj.date() if pd.notna(dt_obj) else None
                    if coerced_record[col_name] is None and raw_value is not None:
                         self.logger.warning(f"Could not parse date '{raw_value}' for column '{col_name}'. Setting to None.")
                elif pyarrow.is_integer_type(pa_type):
                    coerced_record[col_name] = int(float(raw_value)) # Convert to float first to handle "123.0"
                elif pyarrow.is_floating_type(pa_type):
                    coerced_record[col_name] = float(raw_value)
                elif pyarrow.is_boolean_type(pa_type):
                    if isinstance(raw_value, str):
                        if raw_value.lower() in ("true", "t", "1"): coerced_record[col_name] = True
                        elif raw_value.lower() in ("false", "f", "0"): coerced_record[col_name] = False
                        else: coerced_record[col_name] = None # Or raise error
                    else:
                        coerced_record[col_name] = bool(raw_value)
                elif pyarrow.is_string_type(pa_type) and not isinstance(raw_value, str):
                     # If schema expects string but got object/array (e.g. nested JSON)
                    if isinstance(raw_value, (dict, list)):
                        coerced_record[col_name] = json.dumps(raw_value)
                    else:
                        coerced_record[col_name] = str(raw_value)
                else:
                    coerced_record[col_name] = raw_value
            except (ValueError, TypeError, InvalidOperation) as e:
                self.logger.warning(
                    f"Type coercion error for column '{col_name}' with value '{raw_value}' "
                    f"(expected PyArrow type {pa_type}): {e}. Setting to None."
                )
                coerced_record[col_name] = None
        
        # Add any fields present in the record but not in schema (as string, if desired)
        # This might happen if schema discovery was incomplete or data has extra fields.
        # For strict schema adherence, this part can be removed.
        for col_name, raw_value in record.items():
            if col_name not in coerced_record:
                self.logger.debug(f"Column '{col_name}' found in record but not in PyArrow schema. Adding as string.")
                coerced_record[col_name] = str(raw_value) if not isinstance(raw_value, (dict,list)) else json.dumps(raw_value)


        return coerced_record

    def process_record(self, record: dict, context: dict) -> None:
        if not self.stream_initialized:
            self.logger.warning("Stream not initialized in process_record, attempting to initialize.")
            self.start_stream(context)

        if self.output_format == "parquet" and self._pyarrow_schema:
            processed_record = self._coerce_record_to_schema(record, self._pyarrow_schema)
            self.records_buffer.append(processed_record)
        else: # For CSV or if Parquet schema build failed (should have raised earlier)
            self.records_buffer.append(record)


        if self.output_format == "csv":
            # This write-through for CSV can be slow. Consider buffering CSVs too if performance is an issue.
            # For now, keeping your original CSV logic.
            df_single_record = pd.DataFrame([self.records_buffer.pop()]) # Get the record just added
            header = not os.path.exists(self.local_file_path_template) or os.path.getsize(self.local_file_path_template) == 0
            with open(self.local_file_path_template, 'a', newline='', encoding='utf-8') as f:
                df_single_record.to_csv(f, index=False, header=header)
            # records_buffer is already cleared for CSV by popping, or should be empty
            # Ensure CSV logic doesn't double-add or double-clear if process_record also appends.
            # Corrected CSV logic: process_record appends, _drain_batch (or clean_up) writes.
            # For now, to match your code: CSV writes immediately, buffer stays empty.
            # No, let's make CSV consistent with batching logic, it will also buffer.

        # Common batching logic
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
            df = pd.DataFrame(self.records_buffer) # Create DataFrame from (potentially coerced) records

            if self.output_format == "parquet":
                if not self._pyarrow_schema:
                    self.logger.error(f"PyArrow schema not available for Parquet write on stream {self.stream_name}. This should not happen.")
                    # Attempt to build it now as a last resort, or raise
                    self._pyarrow_schema = self._build_pyarrow_schema()
                    if not self._pyarrow_schema:
                        raise ValueError(f"Cannot write Parquet for {self.stream_name} without a PyArrow schema.")

                # Ensure DataFrame columns are in the same order as schema and all schema columns exist
                # This step is crucial for Table.from_pandas with an explicit schema
                final_df_columns = []
                for field in self._pyarrow_schema:
                    if field.name not in df.columns:
                        self.logger.debug(f"Column '{field.name}' from schema not in DataFrame batch for {self.stream_name}. Adding as nulls.")
                        # Add column with Nones, try to infer a compatible Pandas dtype
                        if pyarrow.is_timestamp_type(field.type):
                            df[field.name] = pd.NaT
                        elif pyarrow.is_boolean_type(field.type):
                             df[field.name] = pd.Series([None]*len(df), dtype='boolean') # Pandas nullable boolean
                        elif pyarrow.is_integer_type(field.type) or pyarrow.is_floating_type(field.type) or pyarrow.is_decimal_type(field.type) :
                             df[field.name] = pd.Series([None]*len(df), dtype='float64') #讓pandas/pyarrow處理NaN
                        else: # string, date etc.
                             df[field.name] = pd.Series([None]*len(df), dtype='object')
                    final_df_columns.append(field.name)
                
                # Select columns in schema order
                df_ordered = df[final_df_columns]

                arrow_table = pyarrow.Table.from_pandas(df_ordered, schema=self._pyarrow_schema, safe=True)
                with open(current_local_batch_file, 'wb') as f:
                    pq.write_table(arrow_table, f)

            elif self.output_format == "jsonl":
                with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                    for rec in self.records_buffer: # Use the already coerced records if applicable
                        f.write(json.dumps(self._serialize_json_safe(rec)) + '\n')
            
            elif self.output_format == "csv":
                # Write all buffered records. Header logic needs to be stream-global for CSV if split into batches.
                # For now, single CSV file written in clean_up. This _drain_batch for CSV is if we implement CSV batching.
                # Let's assume clean_up handles the single CSV file for now.
                # If we get here for CSV, it means batch_size_rows was hit.
                # This requires changing how CSV is handled (multiple files or more complex append).
                # For simplicity of this fix, let's assume CSV is only written in clean_up as one file.
                # If CSV batching to multiple files is desired, this part needs more work.
                self.logger.warning(f"CSV batch draining not fully implemented for multiple files. Data remains in buffer for {self.stream_name}.")
                # To avoid losing data if this path is hit unexpectedly:
                # df.to_csv(current_local_batch_file, index=False, header=True) # simplified, header logic more complex for multi-batch CSV
                # Then upload logic...
                # For now, we'll let clean_up handle the single CSV from the buffer.
                return # Skip upload for CSV here, let clean_up handle it.

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
            
            # Clear buffer only if not CSV (as CSV is handled differently for now in _drain_batch)
            if self.output_format != "csv":
                self.records_buffer = []
                self.batch_file_counter += 1

    def _serialize_json_safe(self, record: dict) -> dict:
        """Ensure all values in a record are JSON serializable (e.g. datetimes, decimals)."""
        # This is a shallow copy, modify if deep copy needed
        safe_record = {}
        for key, value in record.items():
            if isinstance(value, (datetime, pd.Timestamp)):
                safe_record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                safe_record[key] = float(value) # Or str(value) for exactness if receiver handles string decimals
            elif isinstance(value, (pd.NaT)): # Handle Pandas NaT
                safe_record[key] = None
            else:
                safe_record[key] = value
        return safe_record


    def format_file_name(self) -> str:
        naming_convention = self.config.get("naming_convention", "{stream}/data_{timestamp}.csv")
        stream_name_safe = re.sub(r'[\\/*?:"<>|]', "_", self.stream_name)
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        file_name_pattern = naming_convention.replace("{stream}", stream_name_safe)
        file_name_pattern = file_name_pattern.replace("{timestamp}", timestamp_str)
        final_path = re.sub(r'[\\*?:"<>|]', "_", file_name_pattern)
        final_path = final_path.strip('/')
        self.logger.debug(f"Formatted base file name/path pattern: {final_path}")
        return final_path

    def clean_up(self) -> None:
        self.logger.info(f"Starting clean_up for stream {self.stream_name}. Records in buffer: {len(self.records_buffer)}")
        if not self.stream_initialized:
            self.logger.warning(f"Stream {self.stream_name} was not initialized. Skipping clean_up.")
            return

        if self.output_format == "csv":
            # CSV: write the entire buffer to the single local file and upload
            if self.records_buffer:
                df = pd.DataFrame(self.records_buffer)
                # Header should be written only if file is new/empty.
                # This assumes one CSV file per stream. If local_file_path_template was created empty in start_stream,
                # os.path.getsize will be 0 initially.
                header = not os.path.exists(self.local_file_path_template) or os.path.getsize(self.local_file_path_template) == 0
                with open(self.local_file_path_template, 'a', newline='', encoding='utf-8') as f: # Append mode
                    df.to_csv(f, index=False, header=header)
                self.logger.info(f"Appended {len(self.records_buffer)} records to CSV {self.local_file_path_template}.")
                self.records_buffer = [] # Clear after writing

            if self.local_file_path_template and os.path.exists(self.local_file_path_template):
                self.logger.info(f"Uploading CSV file {self.local_file_path_template} to {self.blob_path_template}")
                try:
                    blob_client_csv = self.container_client.get_blob_client(blob=self.blob_path_template)
                    with open(self.local_file_path_template, "rb") as data:
                        blob_client_csv.upload_blob(data, overwrite=True)
                    self.logger.info(f"Successfully uploaded CSV {self.blob_path_template}")
                except Exception as e:
                    self.logger.error(f"Failed to upload CSV {self.blob_path_template}: {e}", exc_info=True)
                    raise # Re-raise to signal failure
                finally:
                    try: os.remove(self.local_file_path_template)
                    except Exception as e_rem: self.logger.error(f"Error removing CSV {self.local_file_path_template}: {e_rem}")
            else:
                self.logger.info(f"No CSV data to upload for {self.stream_name} (local file: {self.local_file_path_template} did not exist or buffer was empty).")
                # If no records, consider uploading an empty blob as a marker
                if self.batch_file_counter == 0 and not self.records_buffer: # And buffer is also empty
                    self.logger.info(f"No records processed for CSV stream {self.stream_name}. Uploading empty marker file.")
                    blob_client_csv = self.container_client.get_blob_client(blob=self.blob_path_template)
                    blob_client_csv.upload_blob(b"", overwrite=True)


        elif self.output_format in ["parquet", "jsonl"]:
            if self.records_buffer:
                self.logger.info(f"Draining remaining {len(self.records_buffer)} records for {self.stream_name} ({self.output_format}) during clean_up.")
                self._drain_batch() # This will write, upload, and clear the buffer
            elif self.batch_file_counter == 0: # No records processed at all for this stream
                self.logger.info(f"No records processed and no batches written for {self.stream_name} ({self.output_format}). Writing an empty marker file.")
                current_batch_blob_path = self._get_batch_blob_path() # part_0000...
                current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
                try:
                    if self.output_format == "parquet":
                        if not self._pyarrow_schema: self._pyarrow_schema = self._build_pyarrow_schema()
                        if self._pyarrow_schema:
                            empty_table = self._pyarrow_schema.empty_table()
                            with open(current_local_batch_file, 'wb') as f:
                                pq.write_table(empty_table, f)
                        else: # No schema, can't write typed empty parquet
                             with open(current_local_batch_file, 'wb') as f: pass # Creates 0-byte file
                    elif self.output_format == "jsonl":
                        with open(current_local_batch_file, 'w', encoding='utf-8') as f: pass
                    
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
        self._pyarrow_schema = None # Reset cached schema