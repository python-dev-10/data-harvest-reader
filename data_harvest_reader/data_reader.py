import polars as pl
import os
import zipfile
import io
import re
import sys
from concurrent.futures import ThreadPoolExecutor


class UnsupportedFormatError(Exception):
    pass


class FilterConfigurationError(Exception):
    pass


def _read_csv(file_path, chunksize=None, **kwargs):
    if 'schema_overrides' in kwargs:
        df_cols = set(pl.read_csv(file_path, n_rows=1).columns)
        kwargs['schema_overrides'] = {
            col: dtype
            for col, dtype in kwargs['schema_overrides'].items()
            if col in df_cols
        }
    if chunksize:
        return pl.scan_csv(file_path, raise_if_empty=False, batch_size=chunksize, **kwargs).collect()
    else:
        return pl.read_csv(file_path, raise_if_empty=False, **kwargs)


def _read_parquet(file, n_rows=None, low_memory=False, **kwargs):
    if isinstance(file, (str, bytes, io.BytesIO)):
        lazy_df = pl.read_parquet(file, n_rows=n_rows, low_memory=low_memory, **kwargs)
        return lazy_df
    elif isinstance(file, zipfile.ZipExtFile):
        file_bytes = file.read()
        file_io = io.BytesIO(file_bytes)
        lazy_df = pl.read_parquet(file_io, n_rows=n_rows, low_memory=low_memory, **kwargs)
        return lazy_df
    else:
        raise ValueError("Unsupported file type for Parquet reading")


def _read_excel(file, **kwargs):
    if isinstance(file, zipfile.ZipExtFile):
        file_bytes = file.read()
        file_io = io.BytesIO(file_bytes)
        if 'schema_overrides' in kwargs:
            df_cols = set(pl.read_excel(file_io).columns)
            kwargs['schema_overrides'] = {
                col: dtype
                for col, dtype in kwargs['schema_overrides'].items()
                if col in df_cols
            }
        return pl.read_excel(file_io, **kwargs)
    elif isinstance(file, (str, bytes)):
        if 'schema_overrides' in kwargs:
            df_cols =  set(pl.read_excel(file).columns)
            kwargs['schema_overrides'] = {
                col: dtype
                for col, dtype in kwargs['schema_overrides'].items()
                if col in df_cols
            }
        return pl.read_excel(file, **kwargs)
    else:
        raise ValueError("Unsupported file type for Excel reading")


class DataReader:

    def __init__(self,
                 log_to_file=False,
                 log_file="data_reader.log",
                 logger=None
                 ):
        self.data_formats = {
            '.csv': _read_csv,
            '.parquet': _read_parquet,
            '.xlsx': _read_excel
        }
        self.__available_operations = ('notin', 'in', '==', '>',
                                       '>=', '<', '<=', '!=')
        if not logger:
            from loguru import logger
            self.logger = logger
            self.logger.remove()  # Remove default handlers
            self.logger.add(
                sys.stderr,  # Log to stderr (console)
                format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
                level="INFO",
                colorize=True,
                enqueue=True,  # Enable thread-safe logging
                backtrace=True,  # Enable extended traceback logging
                diagnose=True  # Enable diagnosis information
            )

            if log_to_file:
                self.logger.add(
                    log_file,
                    rotation="1 week",  # New file every week
                    retention="1 month",  # Retain logs for a month
                    level="INFO",  # Minimum level to log
                    format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",  # Log format
                    enqueue=True,  # Enable thread-safe logging
                    backtrace=True,  # Enable extended traceback logging
                    diagnose=True  # Enable diagnosis information
                )
        else:
            self.logger = logger

    def read_data(self, source,
                  join_similar=False,
                  duplicated_subset_dict: dict = None,
                  filter_subset: dict = None,
                  config_df_names: dict = None,
                  override_schema: dict = None,
                  **kwargs):
        self.logger.info("Starting data reading process")

        if config_df_names is None:
            config_df_names = {}

        if isinstance(source, str) and os.path.isdir(source):
            self.logger.info(f"Reading data from directory: {source}")
            data = self._read_from_directory(source, join_similar, config_df_names, override_schema, **kwargs)
        elif isinstance(source, (str, bytes)) and (os.path.isfile(source) or isinstance(source, bytes)):
            self.logger.info("Reading data from zip source")
            data = self._read_from_zip(source, join_similar, config_df_names, override_schema, **kwargs)
        elif isinstance(source, zipfile.ZipFile):
            self.logger.info("Reading data from zipfile.ZipFile object")
            data = self._read_from_zipfile_object(source, join_similar, config_df_names, override_schema, **kwargs)
        else:
            self.logger.error("Unsupported source type: {}", type(source))
            raise ValueError("Unsupported source type")

        if duplicated_subset_dict:
            self.logger.info("Applying deduplication process")
            try:
                data = {
                    f'df_{k}': data[f'df_{k}'].unique(subset=v if v else None, keep='first')
                    if f"df_{k}" in data else data[f'df_{k}']
                    for k, v in duplicated_subset_dict.items()
                }
            except Exception as e:
                self.logger.exception("An error occurred during deduplication")
                raise e

        self.logger.success("Data reading process completed")
        return data

    def _read_from_directory(self, directory_path, join_similar, config_df_names, override_schema, **kwargs):
        files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if
                 os.path.isfile(os.path.join(directory_path, f))]
        return self._read_files_parallel(files, join_similar, config_df_names, override_schema, **kwargs)

    def _read_from_zip(self, zip_source, join_similar, config_df_names, override_schema, **kwargs):
        if isinstance(zip_source, str):
            with zipfile.ZipFile(zip_source, 'r') as zip_ref:
                file_names = zip_ref.namelist()
                files = [zip_ref.open(name) for name in file_names]
        elif isinstance(zip_source, bytes):
            zip_ref = zipfile.ZipFile(io.BytesIO(zip_source), 'r')
            file_names = zip_ref.namelist()
            files = [zip_ref.open(name) for name in file_names]
        else:
            raise ValueError("Invalid zip source type")

        return self._read_files_parallel(files, join_similar, config_df_names, override_schema, **kwargs)

    def _read_from_zipfile_object(self, zipfile_obj, join_similar, config_df_names, override_schema,**kwargs):
        file_names = zipfile_obj.namelist()
        files = [zipfile_obj.open(name) for name in file_names]
        return self._read_files_parallel(files, join_similar, config_df_names, override_schema, **kwargs)

    def _read_files_parallel(self, files, join_similar, config_df_names, override_schema, **kwargs):
        if config_df_names is None:
            config_df_names = {}

        with ThreadPoolExecutor() as executor:
            results = executor.map(lambda f: self._read_file(f, join_similar, config_df_names, override_schema, **kwargs), files)

        dataframes = {}
        for file_name, df in results:
            if df is not None:
                self.logger.info(f"File reading for {file_name} finished")
                base_name = os.path.splitext(os.path.basename(file_name))[0]
                base_name = re.sub(r'_(\d+)|_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|_(\d{14})', '',
                                   base_name) if join_similar else base_name
                key = base_name if base_name.startswith('df_') else config_df_names.get(base_name, f'df_{base_name}')
                if key in dataframes and join_similar:
                    dataframes[key] = pl.concat([dataframes[key], df])
                else:
                    dataframes[key] = df

        return dataframes

    def _read_file(self, file, join_similar, config_df_names, override_schema, **kwargs):
        file_name = file.name if isinstance(file, zipfile.ZipExtFile) else file
        try:
            file_name = os.path.basename(file_name)
            file_id, ext = os.path.splitext(file_name)
            read_func = self.data_formats.get(ext)

            schema_overrides = None

            if ext in ('.csv', '.xlsx') \
                    and override_schema is not None:
                schema_overrides = override_schema.get(file_id.lower(), None)
                if schema_overrides:
                    schema_overrides = {
                        k: pl.Utf8
                        for k, v in schema_overrides.items()
                        if v in ('string', 'Float64', 'Int64')
                    }

            if not read_func:
                raise UnsupportedFormatError(f"Unsupported file format: {ext}")

            self.logger.info(f"Initiating reading of {file_name}")
            if schema_overrides:
                return file_name, read_func(file, schema_overrides=schema_overrides, **kwargs)
            return file_name, read_func(file, **kwargs)
        except Exception as e:
            self.logger.error(f"Error reading file {file_name}: {e}")
            raise
