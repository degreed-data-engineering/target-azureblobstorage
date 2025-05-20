import os
import pandas as pd
from singer_sdk.sinks import RecordSink # Changed from core to sinks for modern SDK
from azure.storage.blob import BlobServiceClient
import re
import logging
import pyarrow # Ensure pyarrow is installed for Parquet
import json # For JSONL
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
import uuid # For unique batch file names

logger = logging.getLogger(__name__) # Use standard Python logging

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming with batching."""

    # Define a default batch size, can be overridden in config
    DEFAULT_BATCH_SIZE_ROWS = 10000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_service_client = None
        self.container_client = None
        # self.blob_client is now set per batch file
        self.local_file_path_template = None # Template for local files if needed for multiple batches
        self.blob_path_template = None # Template for blob paths if needed for multiple batches
        self.stream_initialized = False
        self.output_format = "csv" # Default, will be updated
        # self.logger is already available from the parent class
        self.records_buffer = []
        self.batch_file_counter = 0 # To create unique names for batch files

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
        """Generates a unique suffix for batch files."""
        return f"part_{self.batch_file_counter:04d}_{uuid.uuid4().hex[:8]}"

    def _get_batch_blob_path(self) -> str:
        """Generates the full blob path for the current batch."""
        if self.output_format == "csv": # CSV still appends to one file
            return self.blob_path_template

        base_path, extension = os.path.splitext(self.blob_path_template)
        batch_suffix = self._generate_batch_suffix()
        return f"{base_path}_{batch_suffix}{extension}"

    def _get_batch_local_file_path(self, batch_blob_path: str) -> str:
        """Generates a local temporary path for the current batch file."""
        # Use a subdirectory within /tmp to keep things organized per stream if many files are created
        stream_tmp_dir = os.path.join("/tmp", f"target_azure_{self.stream_name}")
        os.makedirs(stream_tmp_dir, exist_ok=True)
        return os.path.join(stream_tmp_dir, os.path.basename(batch_blob_path))


    def start_stream(self, context: dict) -> None: # context is passed by SDK
        """Initialize the stream (called by SDK)."""
        self.logger.info(f"Starting stream for {self.stream_name}")
        account_name = self.config["storage_account_name"]
        account_key = self.config["storage_account_key"]
        container_name = self.config.get("container_name", "default-container") # Use actual config key
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
            raise # Fail fast if container can't be accessed

        # This template will be used as a base for batch files or the single CSV file
        self.blob_path_template = self.format_file_name()
        self.output_format = self.get_output_format_from_filename(self.blob_path_template)

        if self.output_format == "csv":
            # For CSV, we use a single local file to append to, then upload at the end.
            # Batching for CSV to multiple files is also possible but changes the append logic.
            # For now, keeping CSV as a single upload.
            self.local_file_path_template = os.path.join("/tmp", os.path.basename(self.blob_path_template))
            os.makedirs(os.path.dirname(self.local_file_path_template), exist_ok=True)
            if not os.path.exists(self.local_file_path_template):
                with open(self.local_file_path_template, 'w', encoding='utf-8') as f:
                    f.write('') # Create empty file
        
        self.logger.debug(f"Stream {self.stream_name} initialized. Output format: {self.output_format.upper()}. Batch size: {self.batch_size_rows} rows.")
        self.stream_initialized = True
        self.records_buffer = []
        self.batch_file_counter = 0


    def process_record(self, record: dict, context: dict) -> None:
        """Process record, buffering and flushing in batches for Parquet/JSONL."""
        if not self.stream_initialized:
            # Should be called by SDK before process_record, but as a safeguard:
            self.logger.warning("Stream not initialized in process_record, attempting to initialize.")
            self.start_stream(context)

        self.records_buffer.append(record)

        if self.output_format == "csv":
            # For CSV, we write directly to the single local file by appending
            # This part could also be batched, but it's simpler to append directly
            # and then upload the single CSV at the end.
            # For extreme CSVs, this might still cause /tmp issues if the CSV grows huge before upload.
            # However, the primary memory issue is usually with Parquet/JSONL buffering.
            df = pd.DataFrame([record]) # Create DataFrame for a single record
            # Check if local_file_path_template exists and is empty to decide on header
            header = not os.path.exists(self.local_file_path_template) or os.path.getsize(self.local_file_path_template) == 0
            with open(self.local_file_path_template, 'a', newline='', encoding='utf-8') as f:
                df.to_csv(f, index=False, header=header)
            self.records_buffer = [] # Clear buffer since it's written for CSV

        elif len(self.records_buffer) >= self.batch_size_rows:
            self.logger.info(f"Buffer for {self.stream_name} reached {len(self.records_buffer)} records. Draining batch.")
            self._drain_batch()


    def _drain_batch(self) -> None:
        """Writes the current records_buffer to a batch file and uploads it."""
        if not self.records_buffer:
            self.logger.debug(f"No records in buffer for {self.stream_name}, skipping batch drain.")
            return

        current_batch_blob_path = self._get_batch_blob_path()
        current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
        
        self.logger.info(f"Draining {len(self.records_buffer)} records for {self.stream_name} to local file {current_local_batch_file} for blob {current_batch_blob_path}")

        try:
            if self.output_format == "parquet":
                df = pd.DataFrame(self.records_buffer)
                with open(current_local_batch_file, 'wb') as f:
                    df.to_parquet(f, index=False, engine='pyarrow')
            elif self.output_format == "jsonl":
                with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                    for rec in self.records_buffer:
                        f.write(json.dumps(rec) + '\n')
            else: # Should not happen if logic is correct, but good to handle
                self.logger.error(f"Unsupported format '{self.output_format}' in _drain_batch for {self.stream_name}.")
                return # Or raise error

            file_size = os.path.getsize(current_local_batch_file)
            self.logger.info(f"Successfully wrote batch to {current_local_batch_file}. Size: {file_size} bytes. Uploading to {current_batch_blob_path}.")

            blob_client_for_batch = self.container_client.get_blob_client(blob=current_batch_blob_path)
            with open(current_local_batch_file, "rb") as data:
                blob_client_for_batch.upload_blob(data, overwrite=True) # Consider if overwrite is always desired for batch parts
            self.logger.info(f"Successfully uploaded batch file {current_batch_blob_path}")

        except Exception as e:
            self.logger.error(f"Failed to process or upload batch for {self.stream_name} to {current_batch_blob_path}: {e}", exc_info=True)
            # Depending on error strategy, you might want to re-raise
            raise
        finally:
            if os.path.exists(current_local_batch_file):
                try:
                    os.remove(current_local_batch_file)
                    self.logger.debug(f"Removed local batch file: {current_local_batch_file}")
                except Exception as e_rem:
                    self.logger.error(f"Error removing local batch file {current_local_batch_file}: {e_rem}")
            self.records_buffer = [] # Clear buffer after processing
            self.batch_file_counter += 1


    def format_file_name(self) -> str:
        """Formats the base file name/path pattern for the stream."""
        naming_convention = self.config.get("naming_convention", "{stream}/data_{timestamp}.csv") # Default includes timestamp for uniqueness if not batched
        stream_name_safe = re.sub(r'[\\/*?:"<>|]', "_", self.stream_name) # Sanitize stream name for path
        
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")

        file_name_pattern = naming_convention.replace("{stream}", stream_name_safe)
        file_name_pattern = file_name_pattern.replace("{timestamp}", timestamp_str)
        
        # Basic sanitization for the whole pattern. Users should ensure path components are valid.
        # This is a simplification. For robust path sanitization, each component would be handled.
        final_path = re.sub(r'[\\*?:"<>|]', "_", file_name_pattern)
        # Ensure it doesn't start or end with '/' after sanitization if it's not intended
        final_path = final_path.strip('/')
        
        self.logger.debug(f"Formatted base file name/path pattern: {final_path}")
        return final_path

    def clean_up(self) -> None: # Renamed from finalize to match SDK hook if not using atexit
        """Finalize processing for the stream, uploading any remaining buffered data."""
        self.logger.info(f"Starting clean_up for stream {self.stream_name}. Records in buffer: {len(self.records_buffer)}")

        if not self.stream_initialized:
            self.logger.warning(f"Stream {self.stream_name} was not initialized. Skipping clean_up.")
            return

        # Drain any remaining records in the buffer for Parquet/JSONL
        if self.output_format in ["parquet", "jsonl"]:
            if self.records_buffer: # Only drain if there's something left
                self.logger.info(f"Draining remaining {len(self.records_buffer)} records for {self.stream_name} during clean_up.")
                self._drain_batch()
            else:
                # If no records were ever processed for this stream (or all flushed in batches),
                # we might want to create an empty marker file in Azure.
                # This part is optional and depends on desired behavior for empty streams.
                # For now, if the buffer is empty, _drain_batch does nothing.
                # To ensure an empty file is created if NO records were processed AT ALL for the stream:
                if self.batch_file_counter == 0: # No batches were written yet
                    self.logger.info(f"No records processed and no batches written for {self.stream_name}. Writing an empty marker file.")
                    # Write an empty file (0 records)
                    current_batch_blob_path = self._get_batch_blob_path() # Will use part_0000
                    current_local_batch_file = self._get_batch_local_file_path(current_batch_blob_path)
                    try:
                        if self.output_format == "parquet":
                            df_empty = pd.DataFrame()
                            with open(current_local_batch_file, 'wb') as f:
                                df_empty.to_parquet(f, index=False, engine='pyarrow')
                        elif self.output_format == "jsonl":
                            with open(current_local_batch_file, 'w', encoding='utf-8') as f:
                                pass # Empty file is fine
                        
                        if os.path.exists(current_local_batch_file):
                            blob_client_for_empty = self.container_client.get_blob_client(blob=current_batch_blob_path)
                            with open(current_local_batch_file, "rb") as data:
                                blob_client_for_empty.upload_blob(data, overwrite=True)
                            self.logger.info(f"Successfully uploaded empty marker file {current_batch_blob_path}")
                    except Exception as e:
                         self.logger.error(f"Failed to create or upload empty marker file for {self.stream_name}: {e}", exc_info=True)
                    finally:
                        if os.path.exists(current_local_batch_file):
                             try: os.remove(current_local_batch_file)
                             except: pass


        elif self.output_format == "csv":
            # For CSV, the single local file (self.local_file_path_template) is uploaded here
            if self.local_file_path_template and os.path.exists(self.local_file_path_template):
                self.logger.info(f"Uploading CSV file {self.local_file_path_template} to {self.blob_path_template}")
                try:
                    blob_client_csv = self.container_client.get_blob_client(blob=self.blob_path_template)
                    with open(self.local_file_path_template, "rb") as data:
                        blob_client_csv.upload_blob(data, overwrite=True)
                    self.logger.info(f"Successfully uploaded CSV {self.blob_path_template}")
                except Exception as e:
                    self.logger.error(f"Failed to upload CSV {self.blob_path_template}: {e}", exc_info=True)
                    raise
                finally:
                    try:
                        os.remove(self.local_file_path_template)
                        self.logger.debug(f"Removed local CSV file: {self.local_file_path_template}")
                    except Exception as e_rem:
                        self.logger.error(f"Error removing local CSV file {self.local_file_path_template}: {e_rem}")
            else:
                self.logger.info(f"No CSV data to upload for {self.stream_name} (local file: {self.local_file_path_template}).")
                # Optionally create an empty marker for CSV too if desired
                # blob_client_csv = self.container_client.get_blob_client(blob=self.blob_path_template)
                # blob_client_csv.upload_blob(b"", overwrite=True)
                # self.logger.info(f"Uploaded empty marker CSV file for {self.stream_name}")


        self.logger.info(f"Successfully cleaned up stream for {self.stream_name}")
        # Reset for potential reuse if the sink instance is long-lived (though typically not for CLI runs)
        self.stream_initialized = False
        self.records_buffer = []
        self.batch_file_counter = 0