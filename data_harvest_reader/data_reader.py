import time
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
    if chunksize:
        return pl.scan_csv(file_path, raise_if_empty=False, batch_size=chunksize, **kwargs).collect()
    else:
        return pl.read_csv(file_path, raise_if_empty=False, **kwargs)


def _read_json(file_path, **kwargs):
    return pl.read_json(file_path, **kwargs)


def _read_parquet(file, n_rows=None, low_memory=False, **kwargs):
    try:
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
    except Exception as e:
        logger.error(f"Error reading Parquet file {file}: {e}")
        raise


def _read_excel(file_path, **kwargs):
    return pl.read_excel(file_path, raise_if_empty=False, **kwargs)


class DataReader:

    def __init__(self,
                 log_to_file=False,
                 log_file="data_reader.log",
                 logger=None
                 ):
        self.data_formats = {
            '.csv': _read_csv,
            '.json': _read_json,
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
                  **kwargs):
        self.logger.info("Starting data reading process")

        if config_df_names is None:
            config_df_names = {}

        if isinstance(source, str) and os.path.isdir(source):
            self.logger.info("Reading data from directory: {}", source)
            data = self._read_from_directory(source, join_similar, config_df_names, **kwargs)
        elif isinstance(source, (str, bytes)) and (os.path.isfile(source) or isinstance(source, bytes)):
            self.logger.info("Reading data from zip source")
            data = self._read_from_zip(source, join_similar, config_df_names, **kwargs)
        elif isinstance(source, zipfile.ZipFile):
            self.logger.info("Reading data from zipfile.ZipFile object")
            data = self._read_from_zipfile_object(source, join_similar, config_df_names, **kwargs)
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

        if filter_subset:
            self.logger.info("Applying custom filters")
            try:
                data = {
                    f'df_{k}': self.apply_filters(data[f'df_{k}'], v, f'df_{k}')
                    if f"df_{k}" in data else data[f'df_{k}']
                    for k, v in filter_subset.items()
                }
            except Exception as e:
                self.logger.exception("An error occurred during filtering")
                raise e

        self.logger.success("Data reading process completed")
        return data

    def apply_filters(self, df, filters, df_name):
        try:
            query = pl.lit(True)

            for filter_rule in filters:
                col = filter_rule['column']
                operation = filter_rule['operation']
                values = filter_rule['values']
                operator = filter_rule.get('operator', 'and')

                if operation not in self.__available_operations:
                    msg = (f"{operation} is not allowed, the only allowed operations are "
                           f"'{', '.join(map(str, self.__available_operations))}'")
                    self.logger.exception(msg)
                    raise FilterConfigurationError(msg)

                if operation not in ('notin', 'in') and isinstance(values, list):
                    msg = (f"For list values, use 'notin' or 'in' operation. "
                           f"DataFrame name: {df_name}")
                    self.logger.exception(msg)
                    raise FilterConfigurationError(msg)

                condition = self._build_filter_condition(df, col, operation, values)

                if operator == 'and':
                    query = query & condition
                elif operator == 'or':
                    query = query | condition

            return df.filter(query)
        except Exception as e:
            self.logger.error(f"Error applying filters to {df_name}: {e}")
            raise FilterConfigurationError(f"Error in filter configuration for {df_name}: {e}")

    def _build_filter_condition(self, df, column, operation, values):
        if operation == '==':
            condition = df[column] == values
        elif operation == '>':
            condition = df[column] > values
        elif operation == '>=':
            condition = df[column] >= values
        elif operation == '<':
            condition = df[column] < values
        elif operation == '<=':
            condition = df[column] <= values
        elif operation == '!=':
            condition = df[column] != values
        elif operation == 'in':
            values = values if isinstance(values, list) else [values]
            condition = df[column].is_in(values)
        elif operation == 'notin':
            values = values if isinstance(values, list) else [values]
            condition = ~df[column].is_in(values)
        else:
            raise FilterConfigurationError(f"Unsupported operation: {operation}")

        return condition

    def _read_from_directory(self, directory_path, join_similar, config_df_names, **kwargs):
        files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if
                 os.path.isfile(os.path.join(directory_path, f))]
        return self._read_files_parallel(files, join_similar, config_df_names, **kwargs)

    def _read_from_zip(self, zip_source, join_similar, config_df_names, **kwargs):
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

        return self._read_files_parallel(files, join_similar, config_df_names, **kwargs)

    def _read_from_zipfile_object(self, zipfile_obj, join_similar, config_df_names, **kwargs):
        file_names = zipfile_obj.namelist()
        files = [zipfile_obj.open(name) for name in file_names]
        return self._read_files_parallel(files, join_similar, config_df_names, **kwargs)

    def _read_files_parallel(self, files, join_similar, config_df_names, **kwargs):
        if config_df_names is None:
            config_df_names = {}

        with ThreadPoolExecutor() as executor:
            results = executor.map(lambda f: self._read_file(f, join_similar, config_df_names, **kwargs), files)

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

    def _read_file(self, file, join_similar, config_df_names, **kwargs):
        file_name = file.name if isinstance(file, zipfile.ZipExtFile) else file
        try:
            ext = os.path.splitext(file_name)[1]
            read_func = self.data_formats.get(ext)

            if not read_func:
                raise UnsupportedFormatError(f"Unsupported file format: {ext}")

            self.logger.info(f"Initiating reading of {file_name}")
            return file_name, read_func(file, **kwargs)
        except Exception as e:
            self.logger.error(f"Error reading file {file_name}: {e}")
            raise  # Re-raise other exceptions
