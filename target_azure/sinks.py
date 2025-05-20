import os
import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient
import re
import logging
import pyarrow
import pyarrow.parquet as pq
import pyarrow.types 
import json
from azure.core.exceptions import ResourceExistsError
from datetime import datetime, date 
import uuid
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP 
from typing import Optional, Union, Dict, Any 

logger = logging.getLogger(__name__)

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming with batching."""

    DEFAULT_BATCH_SIZE_ROWS = 10000
    DEFAULT_DECIMAL_PRECISION = 38 
    DEFAULT_DECIMAL_SCALE = 9      

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_service_client: Optional[BlobServiceClient] = None
        self.container_client: Optional[Any] = None
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
            return self.blob_path_template or ""
        
        if not self.blob_path_template:
             self.logger.error("blob_path_template is not set in _get_batch_blob_path")
             return f"{self.stream_name}/error_unknown_path_{self._generate_batch_suffix()}.{self.output_format}"

        base_path, extension = os.path.splitext(self.blob_path_template)
        batch_suffix = self._generate_batch_suffix()
        return f"{base_path}_{batch_suffix}{extension}"

    def _get_batch_local_file_path(self, batch_blob_path: str) -> str:
        stream_tmp_dir = os.path.join("/tmp", f"target_azure_{self.stream_name}")
        os.makedirs(stream_tmp_dir, exist_ok=True)
        return os.path.join(stream_tmp_dir, os.path.basename(batch_blob_path))

    def _singer_type_to_pyarrow_type(self, singer_type_def: Dict[str, Any], property_name: str) -> Optional[pyarrow.DataType]:
        singer_types = singer_type_def.get("type", ["null", "string"])
        
        if "number" in singer_types:
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
            if isinstance(item_type_def, dict):
                pa_item_type = self._singer_type_to_pyarrow_type(item_type_def, f"{property_name}_items")
                if pa_item_type:
                    return pyarrow.list_(pa_item_type)
            return pyarrow.list_(pyarrow.string())
        elif "object" in singer_types:
            return pyarrow.string() 

        return pyarrow.string()

    def _build_pyarrow_schema(self) -> Optional[pyarrow.Schema]:
        if self._pyarrow_schema:
            return self._pyarrow_schema

        if not self.schema:
            self.logger.error(f"Singer schema (self.schema) not available for stream {self.stream_name}.")
            return None

        fields = []
        for property_name, property_definition in self.schema.get("properties", {}).items():
            pa_type = self._singer_type_to_pyarrow_type(property_definition, property_name)
            if pa_type:
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
        self._pyarrow_schema = None 
        
        account_name = self.config["storage_account_name"]
        account_key = self.config["storage_account_key"]
        container_name = self.config.get("container_name", "default-container")
        connection_string = self.config.get(
            "azure_storage_connection_string",
            f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        )

        self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        if self.blob_service_client:
            self.container_client = self.blob_service_client.get_container_client(container_name)
        else:
            self.logger.critical("Failed to create BlobServiceClient.")
            raise ConnectionError("Failed to create BlobServiceClient.")

        try:
            if self.container_client:
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
            if not self._build_pyarrow_schema(): 
                msg = f"Failed to build PyArrow schema for Parquet output on stream {self.stream_name}. Cannot proceed."
                self.logger.critical(msg)
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
        coerced_record: Dict[str, Any] = {}
        for field in pa_schema:
            col_name = field.name
            pa_type = field.type
            raw_value = record.get(col_name)

            if raw_value is None:
                coerced_record[col_name] = None
                continue

            try:
                if pyarrow.types.is_decimal(pa_type):
                    str_val = str(raw_value).strip()
                    if not str_val:
                        coerced_record[col_name] = None
                    else:
                        try:
                            original_decimal = Decimal(str_val)
                            target_scale = pa_type.scale 
                            quantizer = Decimal('1e-' + str(target_scale))
                            coerced_decimal = original_decimal.quantize(quantizer, rounding=ROUND_HALF_UP)
                            coerced_record[col_name] = coerced_decimal
                        except InvalidOperation:
                            coerced_record[col_name] = None
                elif pyarrow.types.is_timestamp(pa_type):
                    dt_val = pd.to_datetime(raw_value, errors='coerce', utc=False)
                    if pd.isna(dt_val):
                        coerced_record[col_name] = None
                    else:
                        if pa_type.tz:
                             if dt_val.tzinfo is None:
                                 coerced_record[col_name] = dt_val.tz_localize(pa_type.tz)
                             else:
                                 coerced_record[col_name] = dt_val.tz_convert(pa_type.tz)
                        else:
                             coerced_record[col_name] = dt_val.tz_localize(None) if dt_val.tzinfo else dt_val
                elif pyarrow.types.is_date(pa_type):
                    dt_obj = pd.to_datetime(raw_value, errors='coerce')
                    coerced_record[col_name] = dt_obj.date() if pd.notna(dt_obj) else None
                elif pyarrow.types.is_integer(pa_type):
                    coerced_record[col_name] = int(float(str(raw_value)))
                elif pyarrow.types.is_floating(pa_type):
                    coerced_record[col_name] = float(str(raw_value))
                elif pyarrow.types.is_boolean(pa_type):
                    if isinstance(raw_value, str):
                        val_lower = raw_value.lower()
                        if val_lower in ("true", "t", "1", "yes", "y"): coerced_record[col_name] = True
                        elif val_lower in ("false", "f", "0", "no", "n"): coerced_record[col_name] = False
                        else: coerced_record[col_name] = None
                    else:
                        coerced_record[col_name] = bool(raw_value)
                elif pyarrow.types.is_list(pa_type):
                    if isinstance(raw_value, list):
                        if pyarrow.types.is_string(pa_type.value_type) or pyarrow.types.is_large_string(pa_type.value_type):
                            coerced_list = []
                            for item in raw_value:
                                if isinstance(item, (dict, list)): 
                                    coerced_list.append(json.dumps(item))
                                elif item is not None:
                                    coerced_list.append(str(item))
                                else:
                                    coerced_list.append(None)
                            coerced_record[col_name] = coerced_list
                        else:
                            # For lists of other non-string types, pass through.
                            # More robust: recursively coerce items based on pa_type.value_type.
                            coerced_record[col_name] = raw_value 
                    else: 
                        coerced_record[col_name] = None 
                elif pyarrow.types.is_string(pa_type) or pyarrow.types.is_large_string(pa_type):
                    if isinstance(raw_value, (dict, list)):
                        coerced_record[col_name] = json.dumps(raw_value)
                    else:
                        coerced_record[col_name] = str(raw_value)
                else:
                    coerced_record[col_name] = raw_value
            except (ValueError, TypeError, InvalidOperation, OverflowError) as e:
                coerced_record[col_name] = None
        
        for col_name, raw_value in record.items():
            if col_name not in coerced_record:
                if isinstance(raw_value, (dict,list)):
                    try:
                        coerced_record[col_name] = json.dumps(raw_value)
                    except TypeError:
                        coerced_record[col_name] = str(raw_value)
                elif raw_value is not None:
                    coerced_record[col_name] = str(raw_value)
                else:
                    coerced_record[col_name] = None
        return coerced_record

    def process_record(self, record: Dict[str, Any], context: Dict[str, Any]) -> None:
        if not self.stream_initialized:
            self.logger.warning(f"Stream {self.stream_name} not initialized in process_record, attempting to initialize. This is unexpected.")
            self.start_stream(context) 

        if self.output_format == "parquet":
            if not self._pyarrow_schema:
                self.logger.error(f"PyArrow schema missing for stream {self.stream_name} in process_record. Rebuilding.")
                if not self._build_pyarrow_schema():
                    self.logger.critical(f"Could not build PyArrow schema for {self.stream_name} on the fly. Record processing aborted for this stream.")
                    return 
            processed_record = self._coerce_record_to_schema(record, self._pyarrow_schema)
            self.records_buffer.append(processed_record)
        else: 
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
            df = pd.DataFrame(self.records_buffer)

            if self.output_format == "parquet":
                if not self._pyarrow_schema:
                     raise ValueError(f"Parquet schema not built for stream {self.stream_name} before drain.")

                data_for_arrow = {}
                df_cols = df.columns
                for field in self._pyarrow_schema:
                    col_name = field.name
                    if col_name in df_cols:
                        data_for_arrow[col_name] = df[col_name]
                    else:
                        pd_dtype = 'object'
                        if pyarrow.types.is_timestamp(field.type): pd_dtype = 'datetime64[ns]'
                        elif pyarrow.types.is_boolean(field.type): pd_dtype = 'boolean'
                        elif pyarrow.types.is_integer(field.type): pd_dtype = 'Int64'
                        elif pyarrow.types.is_floating(field.type) or pyarrow.types.is_decimal(field.type): pd_dtype = 'float64'
                        data_for_arrow[col_name] = pd.Series([None] * len(df), dtype=pd_dtype, name=col_name)

                df_aligned = pd.DataFrame(data_for_arrow, columns=[field.name for field in self._pyarrow_schema])

                arrow_table = pyarrow.Table.from_pandas(df_aligned, schema=self._pyarrow_schema, safe=True, preserve_index=False)
                with open(current_local_batch_file, 'wb') as f:
                    pq.write_table(arrow_table, f)

            elif self.output_format == "jsonl":
                with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                    for rec in self.records_buffer:
                        f.write(json.dumps(self._serialize_json_safe(rec)) + '\n')
            
            elif self.output_format == "csv":
                is_first_batch_file = self.batch_file_counter == 0
                df.to_csv(current_local_batch_file, index=False, header=is_first_batch_file, encoding='utf-8', lineterminator='\n')

            file_size = os.path.getsize(current_local_batch_file)
            self.logger.info(f"Successfully wrote batch to {current_local_batch_file}. Size: {file_size} bytes. Uploading to {current_batch_blob_path}.")

            if self.container_client:
                blob_client_for_batch = self.container_client.get_blob_client(blob=current_batch_blob_path)
                with open(current_local_batch_file, "rb") as data:
                    blob_client_for_batch.upload_blob(data, overwrite=True)
                self.logger.info(f"Successfully uploaded batch file {current_batch_blob_path}")
            else:
                self.logger.error("Container client not initialized, cannot upload batch.")
                raise ConnectionError("Azure container client not initialized.")

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
            
            self.records_buffer = []
            self.batch_file_counter += 1

    def _serialize_json_safe(self, record: Dict[str, Any]) -> Dict[str, Any]:
        safe_record: Dict[str, Any] = {}
        for key, value in record.items():
            if isinstance(value, (datetime, pd.Timestamp, date)):
                safe_record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                safe_record[key] = float(value) 
            elif pd.isna(value):
                safe_record[key] = None
            else:
                safe_record[key] = value
        return safe_record

    def format_file_name(self) -> str:
        naming_convention = self.config.get("naming_convention", "{stream}/{stream}_{timestamp}.csv")
        stream_name_safe = re.sub(r'[\\/*?:"<>|]', "_", self.stream_name)
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
        
        file_name_pattern = naming_convention.replace("{stream}", stream_name_safe)
        if "{timestamp}" in file_name_pattern:
            file_name_pattern = file_name_pattern.replace("{timestamp}", timestamp_str)
        
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

        if self.records_buffer:
            self.logger.info(f"Draining remaining {len(self.records_buffer)} records for {self.stream_name} ({self.output_format}) during clean_up.")
            self._drain_batch()
        
        elif self.batch_file_counter == 0: 
            self.logger.info(f"No records processed and no batches written for {self.stream_name} ({self.output_format}). Writing an empty marker file.")
            current_batch_blob_path = self._get_batch_blob_path() 
            current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
            try:
                if self.output_format == "parquet":
                    if not self._pyarrow_schema: self._pyarrow_schema = self._build_pyarrow_schema()
                    if self._pyarrow_schema:
                        empty_table = self._pyarrow_schema.empty_table()
                        with open(current_local_batch_file, 'wb') as f:
                            pq.write_table(empty_table, f)
                    else:
                         self.logger.warning(f"Cannot write typed empty Parquet for {self.stream_name} due to missing schema. Writing 0-byte file.")
                         with open(current_local_batch_file, 'wb') as f: pass
                elif self.output_format == "jsonl":
                    with open(current_local_batch_file, 'w', encoding='utf-8') as f: pass
                elif self.output_format == "csv":
                    with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                        if self.schema and "properties" in self.schema:
                            header_row = ",".join(self.schema["properties"].keys())
                            f.write(header_row + "\n")
                
                if os.path.exists(current_local_batch_file) and self.container_client:
                    blob_client_empty = self.container_client.get_blob_client(blob=current_batch_blob_path)
                    with open(current_local_batch_file, "rb") as data:
                        blob_client_empty.upload_blob(data, overwrite=True)
                    self.logger.info(f"Successfully uploaded empty marker file {current_batch_blob_path}")
            except Exception as e:
                self.logger.error(f"Failed to create or upload empty marker file for {self.stream_name}: {e}", exc_info=True)
            finally:
                if os.path.exists(current_local_batch_file):
                    try: os.remove(current_local_batch_file)
                    except Exception as e_rem_empty: 
                        self.logger.error(f"Error removing empty local file {current_local_batch_file}: {e_rem_empty}")
        
        self.logger.info(f"Successfully cleaned up stream for {self.stream_name}")
        self.stream_initialized = False
        self.records_buffer = []
        self.batch_file_counter = 0
        self._pyarrow_schema = None