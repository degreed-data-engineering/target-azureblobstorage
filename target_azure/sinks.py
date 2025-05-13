import os
import pandas as pd
from singer_sdk.sinks import RecordSink
from azure.storage.blob import BlobServiceClient # Removed BlobClient as get_blob_client returns it
import re
import logging
import pyarrow
from azure.core.exceptions import ResourceExistsError
from datetime import datetime
# import atexit

class TargetAzureBlobSink(RecordSink):
    """Azure Storage target sink class for streaming."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blob_service_client = None
        self.container_client = None
        self.blob_client = None
        self.local_file_path = None
        self.stream_initialized = False
        self.output_format = "csv" # Default output format
        self.logger.setLevel(logging.DEBUG)
        # atexit.register(self.finalize) # Be careful with atexit in long-running or complex apps

    def get_output_format_from_filename(self, filename: str) -> str:
        """Determines the output format based on the file extension."""
        if filename.lower().endswith(".parquet"):
            return "parquet"
        elif filename.lower().endswith(".jsonl") or filename.lower().endswith(".json"): # Treat .json as jsonl for simplicity here
            return "jsonl"
        # Add other formats like .json if you want to support standard JSON arrays later
        return "csv" # Default to CSV

    def start_stream(self) -> None:
        """Initialize the stream."""
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

        file_name_with_path = self.format_file_name() # This now includes the full desired blob path
        self.output_format = self.get_output_format_from_filename(file_name_with_path)

        self.blob_path = file_name_with_path # format_file_name now returns the full path
        self.local_file_path = os.path.join("/tmp", os.path.basename(file_name_with_path)) # Use only basename for local temp file
        os.makedirs(os.path.dirname(self.local_file_path), exist_ok=True)

        # For Parquet, we don't create an empty file initially like for CSV appending.
        # For JSONL, we also don't need an empty file to start, each line is a new object.
        # For CSV, the original logic is fine.
        if self.output_format == "csv":
            if not os.path.exists(self.local_file_path):
                with open(self.local_file_path, 'w') as f: # For CSV, ensure it's writable text mode
                    f.write('')
        elif os.path.exists(self.local_file_path): # If not CSV and file exists from previous run (should not happen with /tmp typically)
            os.remove(self.local_file_path)

        self.blob_client = self.container_client.get_blob_client(blob=self.blob_path)
        self.logger.debug(f"Initialized blob client for: {self.blob_path}. Output format: {self.output_format.upper()}")
        self.stream_initialized = True
        self.records_buffer = [] # Buffer records for Parquet/JSONL

    def process_record(self, record: dict, context: dict) -> None:
        """Process and write the record."""
        if not self.stream_initialized:
            self.start_stream()


        if self.output_format == "csv":
            df = pd.DataFrame([record])
            header = not os.path.exists(self.local_file_path) or os.path.getsize(self.local_file_path) == 0
            # Open in append text mode for CSV
            with open(self.local_file_path, 'a', newline='', encoding='utf-8') as f:
                df.to_csv(f, index=False, header=header)

        else:
            # For Parquet and JSONL, buffer records and write in batches (or at the end)
            self.records_buffer.append(record)


    def format_file_name(self) -> str:
        """
        Format the file name based on the context and Azure Blob Storage naming rules.
        The naming_convention can now include path components.
        Example: "my_subfolder/{stream}_{timestamp}.parquet"
        """
        # Default naming convention is now more generic until format is determined
        naming_convention = self.config.get("naming_convention", "{stream}/data.csv")
        stream_name = self.stream_name

        # Basic timestamp for uniqueness if not in naming_convention
        # More robust timestamping from singer_sdk.helpers._typing might be better
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")

        # Simple replacement, consider more robust templating if needed
        file_name_pattern = naming_convention.replace("{stream}", stream_name)
        file_name_pattern = file_name_pattern.replace("{timestamp}", timestamp_str)
        # Add other placeholders like {date}, {time} if you have them

        # Replace or remove invalid characters for Azure Blob Storage (applies to path components too)
        # This regex is a bit aggressive for path components; Azure allows '/'
        # Let's refine it to only sanitize the actual filename part if '/' is a path separator
        
        # Assume '/' is a path separator and should not be sanitized
        # Sanitize each component if you want to be super careful, or just the basename
        # For simplicity, we'll sanitize the whole thing but this might mangle desired subfolders
        # if they contain invalid chars. Better to ensure naming_convention is clean.
        # A more robust way would be to split by '/', sanitize each part except the last,
        # then sanitize the last part (filename).
        
        # Simplified: User must ensure path in naming_convention is valid. Sanitize only filename chars.
        path_parts = file_name_pattern.split('/')
        sanitized_parts = []
        for i, part in enumerate(path_parts):
            if i == len(path_parts) - 1: # Last part is the filename
                 # Replace invalid characters for the filename part
                sanitized_parts.append(re.sub(r'[\\*?:"<>|]', "_", part))
            else: # Path components
                # Path components can have different rules, but let's be safe
                # Azure allows most chars in paths, but let's avoid these common ones too.
                sanitized_parts.append(re.sub(r'[\\*?:"<>|]', "_", part))
        
        final_path = "/".join(sanitized_parts)

        self.logger.debug(f"Formatted file name/path: {final_path}")
        return final_path

    def finalize_buffered_data(self):
        """Writes buffered data to local file for Parquet or JSONL."""
        self.logger.info(f"Entering finalize_buffered_data. Record count: {len(self.records_buffer)}. Local file path: {self.local_file_path}") # NEW LOG

        if not self.records_buffer:
            self.logger.info(f"No records buffered for {self.stream_name}, attempting to write empty file to {self.local_file_path}.")
            if not os.path.exists(self.local_file_path):
                 with open(self.local_file_path, 'wb') as f:
                    if self.output_format == "parquet":
                        self.logger.debug(f"Writing empty schema-only parquet file to {self.local_file_path}.")
                        # Create an empty DataFrame WITH SCHEMA if possible, else just empty
                        # schema_cols = list(self.schema["properties"].keys()) if self.schema and "properties" in self.schema else []
                        # df_empty = pd.DataFrame(columns=schema_cols)
                        df_empty = pd.DataFrame() # Simplest empty Parquet
                        df_empty.to_parquet(f, index=False, engine='pyarrow')
                    elif self.output_format == "jsonl":
                         self.logger.debug(f"Writing empty JSONL file to {self.local_file_path}.")
                         pass # Empty file is fine
            else:
                self.logger.info(f"Local file {self.local_file_path} already exists (empty records case). Doing nothing to it here.")
            return

        # If records were buffered:
        df = pd.DataFrame(self.records_buffer) # This line could fail if records_buffer contains problematic data for DataFrame creation
        self.logger.info(f"Writing {len(self.records_buffer)} buffered records to {self.local_file_path} as {self.output_format.upper()}")

        if self.output_format == "parquet":
            try:
                self.logger.debug(f"Attempting to write Parquet data to {self.local_file_path}")
                with open(self.local_file_path, 'wb') as f:
                    df.to_parquet(f, index=False, engine='pyarrow')
                self.logger.info(f"Successfully wrote Parquet data to {self.local_file_path}. File size: {os.path.getsize(self.local_file_path)} bytes.") # NEW LOG
            except Exception as e:
                self.logger.error(f"Error writing Parquet file {self.local_file_path}: {e}", exc_info=True) # Add exc_info
                raise
        elif self.output_format == "jsonl":
            try:
                self.logger.debug(f"Attempting to write JSONL data to {self.local_file_path}")
                with open(self.local_file_path, 'w', encoding='utf-8') as f:
                    for record_item in self.records_buffer: # Renamed to avoid conflict
                        import json
                        f.write(json.dumps(record_item) + '\n')
                self.logger.info(f"Successfully wrote JSONL data to {self.local_file_path}. File size: {os.path.getsize(self.local_file_path)} bytes.") # NEW LOG
            except Exception as e:
                self.logger.error(f"Error writing JSONL file {self.local_file_path}: {e}", exc_info=True) # Add exc_info
                raise
        
        self.records_buffer = []


    def clean_up(self) -> None:
        self.logger.info(f"Starting clean_up for stream {self.stream_name}. Stream initialized: {self.stream_initialized}. Local file path: {self.local_file_path}. Output format: {self.output_format}") # NEW LOG

        if not self.stream_initialized:
            self.logger.warning(f"Stream {self.stream_name} was not initialized. Skipping clean_up actions.")
            return

        if self.output_format in ["parquet", "jsonl"]:
            self.logger.debug("Calling finalize_buffered_data()")
            self.finalize_buffered_data()
            self.logger.debug("Returned from finalize_buffered_data()")


        if not self.local_file_path:
            self.logger.error("local_file_path is None during clean_up after finalize_buffered_data. This should not happen.")
            return

        self.logger.info(f"Post finalize_buffered_data: Checking existence of local file: {self.local_file_path}") # NEW LOG

        try:
            if not os.path.exists(self.local_file_path):
                self.logger.warning(f"Local file {self.local_file_path} does not exist after finalize_buffered_data. Skipping upload for {self.blob_path}.")
                return
            
            file_size = os.path.getsize(self.local_file_path)
            if file_size == 0:
                self.logger.info(f"Local file {self.local_file_path} is empty (0 bytes). Proceeding with upload of empty file to {self.blob_path}.")
                # Decide if you want to upload genuinely empty files or skip.
                # For now, let's upload it.
            else:
                self.logger.info(f"Local file {self.local_file_path} exists. Size: {file_size} bytes.")


            self.logger.debug(f"Preparing to upload {self.local_file_path} (as {self.output_format.upper()}) to Azure Blob Storage path: {self.blob_path}")
            with open(self.local_file_path, "rb") as data:
                self.blob_client.upload_blob(data, overwrite=True) # Ensure self.blob_client is valid
            self.logger.info(f"Successfully uploaded {self.blob_path} to Azure Blob Storage")
        except Exception as e:
            self.logger.error(f"Failed during upload or pre-upload check for {self.blob_path}: {e}", exc_info=True) # Add exc_info
            # Decide on re-raise. If clean_up is the very last step, maybe just log.
            # If the SDK expects clean_up to raise on failure, then do:
            # raise
        finally:
            # ... (cleanup of local file)
            if self.local_file_path and os.path.exists(self.local_file_path):
                try:
                    os.remove(self.local_file_path)
                    self.logger.debug(f"Removed local file: {self.local_file_path}")
                except Exception as e:
                    self.logger.error(f"Error removing local file {self.local_file_path}: {e}", exc_info=True)

    def finalize(self) -> None:
        """Upload the local file to Azure Blob Storage and remove it."""
        self.logger.info(f"Finalizing stream for {self.stream_name}")

        if not self.stream_initialized:
            self.logger.info(f"Stream {self.stream_name} was not initialized (e.g. no records). Skipping finalize.")
            return

        # If data was buffered (Parquet/JSONL), write it to local file now
        if self.output_format in ["parquet", "jsonl"]:
            self.finalize_buffered_data()

        if not self.local_file_path: # Should be set in start_stream
            self.logger.error("local_file_path is None during finalize.")
            return

        self.logger.debug(f"Preparing to upload {self.local_file_path} (as {self.output_format.upper()}) to Azure Blob Storage path: {self.blob_path}")

        try:
            if not os.path.exists(self.local_file_path) or os.path.getsize(self.local_file_path) == 0:
                if not self.records_buffer: # If buffer was also empty and file is empty/non-existent
                     self.logger.info(f"Local file {self.local_file_path} is empty or does not exist, and no records were buffered. Skipping upload for {self.blob_path}.")
                     # Optionally, create an empty blob to represent an empty extract
                     # self.blob_client.upload_blob(b"", overwrite=True)
                     # self.logger.info(f"Uploaded empty blob to {self.blob_path} to signify no data.")
                     return # Don't try to upload a non-existent or truly empty file unless intended
                # If buffer was processed but file is still empty, it's an issue.
                # The finalize_buffered_data should handle creating an empty file if needed.


            with open(self.local_file_path, "rb") as data: # Always read as binary for upload
                self.blob_client.upload_blob(data, overwrite=True)
            self.logger.info(f"Successfully uploaded {self.blob_path} to Azure Blob Storage")
        except Exception as e:
            self.logger.error(f"Failed to upload {self.blob_path} to Azure Blob Storage: {e}")
            # Do not re-raise here if atexit is used, as it can mask other issues
            # Let atexit handler complete. If this is critical, consider removing atexit
            # and ensuring finalize is called explicitly by the SDK's lifecycle.
            # For now, just log it. If part of SDK's clean_up, then re-raising is fine.
            # Given it's an atexit, let's not re-raise to avoid masking other shutdown errors.
            # raise
        finally:
            if self.local_file_path and os.path.exists(self.local_file_path):
                try:
                    os.remove(self.local_file_path)
                    self.logger.debug(f"Removed local file: {self.local_file_path}")
                except Exception as e:
                    self.logger.error(f"Error removing local file {self.local_file_path}: {e}")
            # else: # This log can be noisy if file was intentionally not created (e.g. no records)
            #     self.logger.warning(f"Local file not found during cleanup: {self.local_file_path}")

        self.logger.info(f"Successfully finalized stream for {self.stream_name}")

# Main execution block (if run as script) - usually not needed when used as a library/plugin
# if __name__ == "__main__":
#     TargetAzureBlobSink.cli()