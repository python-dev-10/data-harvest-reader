import time

import polars as pl
import os
import zipfile
import io
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from loguru import logger


class UnsupportedFormatError(Exception):
    pass


class FilterConfigurationError(Exception):
    pass


def _read_csv(file_path, chunksize=None, **kwargs):
    """
    The _read_csv function reads a csv file and returns the data as a pandas DataFrame.

    :param self: Represent the instance of the class
    :param file_path: Specify the path to the file that we want to read
    :param chunksize: Specify the size of each chunk
    :param **kwargs: Pass in any additional parameters that may be required by the function
    :return: A pandas dataframe
    :doc-author: Trelent
    """
    if chunksize:
        return pl.scan_csv(file_path, batch_size=chunksize, **kwargs).collect()
    else:
        return pl.read_csv(file_path, **kwargs)


def _read_json(file_path, **kwargs):
    """
    The _read_json function reads a JSON file and returns the data as a Pandas DataFrame.

    :param self: Represent the instance of the class
    :param file_path: Specify the location of the file to be read
    :param **kwargs: Pass a variable number of keyword arguments to a function
    :return: A pandas dataframe
    :doc-author: Trelent
    """
    return pl.read_json(file_path, **kwargs)


def _read_parquet(file_path, n_rows=None, low_memory=False, **kwargs):
    """
    The _read_parquet function reads a Parquet file into a Pandas DataFrame.
    :param file_path: Specify the path to the file that is being read
    :param n_rows: Limit the number of rows read from a file
    :param low_memory: Determine whether to use a buffer when reading the data
    :param **kwargs: Pass keyworded, variable-length argument list to a function
    :return: A dataframe
    """
    try:
        lazy_df = pl.scan_parquet(file_path, n_rows=n_rows, low_memory=low_memory, **kwargs)
        return lazy_df.collect()  # Collect the data into a DataFrame
    except Exception as e:
        logger.error(f"Error reading Parquet file {file_path}: {e}")
        raise


def _read_excel(file_path, **kwargs):
    """
    The _read_excel function reads in an excel file and returns a pandas dataframe.
    :param file_path: Specify the path of the file to be read
    :param **kwargs: Pass a variable number of keyword arguments to the function
    :return: A pandas dataframe
    """
    return pl.read_excel(file_path, **kwargs)


class DataReader:

    def __init__(self, log_to_file=False, log_file="data_reader.log"):
        """
        :param log_to_file: Determine if the logger should log to a file
        :param log_file: Specify the name of the log file
        :return: Nothing, but it does set up the logger
        """
        self.data_formats = {
                '.csv': _read_csv,
                '.json': _read_json,
                '.parquet': _read_parquet,
                '.xlsx': _read_excel
            }
        self.__available_operations = ('notin', 'in', '==', '>',
                                       '>=', '<', '<=', '!=')
        logger.remove()  # Remove default handlers
        logger.add(
            sys.stderr,  # Log to stderr (console)
            format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",
            level="INFO",
            colorize=True,
            enqueue=True,  # Enable thread-safe logging
            backtrace=True,  # Enable extended traceback logging
            diagnose=True  # Enable diagnosis information
        )

        if log_to_file:
            logger.add(
                log_file,
                rotation="1 week",  # New file every week
                retention="1 month",  # Retain logs for a month
                level="INFO",  # Minimum level to log
                format="{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}",  # Log format
                enqueue=True,  # Enable thread-safe logging
                backtrace=True,  # Enable extended traceback logging
                diagnose=True  # Enable diagnosis information
            )

    def read_data(self, source,
                  join_similar=False,
                  duplicated_subset_dict: dict = None,
                  filter_subset: dict = None,
                  **kwargs):
        """
        The read_data function reads data from a directory or ZIP file and returns a dictionary of pandas DataFrames.
        :param source: Specify the path to a directory or zip file
        :param join_similar: Join the dataframes that have similar columns
        :param duplicated_subset_dict: dict: Remove duplicates from the dataframe
        :param filter_subset: dict: Apply custom filters to the dataframes
        :param **kwargs: Pass keyword arguments to the function
        :return: A dictionary of dataframes
        """
        logger.info("Starting data reading process")

        # Check if the source is a string path to a directory
        if isinstance(source, str) and os.path.isdir(source):
            logger.info("Reading data from directory: {}", source)
            data = self._read_from_directory(source, join_similar, **kwargs)

        # Check if the source is a string path to a ZIP file or ZIP file bytes
        elif isinstance(source, (str, bytes)) and (os.path.isfile(source) or isinstance(source, bytes)):
            logger.info("Reading data from zip source")
            data = self._read_from_zip(source, join_similar, **kwargs)

        # Raise an error if the source is neither a directory nor a ZIP file
        else:
            logger.error("Unsupported source type: {}", type(source))
            raise ValueError("Unsupported source type")

        if duplicated_subset_dict:
            logger.info("Applying deduplication process")
            try:
                data = {
                    f'df_{k}': data[f'df_{k}'].unique(subset=v if v else None, keep='first')
                    if f"df_{k}" in data else data[f'df_{k}']
                    for k, v in duplicated_subset_dict.items()
                }
            except Exception as e:
                logger.exception("An error occurred during deduplication")
                raise e

        if filter_subset:
            logger.info("Applying custom filters")
            try:
                data = {
                    f'df_{k}': self.apply_filters(data[f'df_{k}'], v, f'df_{k}')
                    if f"df_{k}" in data else data[f'df_{k}']
                    for k, v in filter_subset.items()
                }
            except Exception as e:
                logger.exception("An error occurred during filtering")
                raise e

        logger.success("Data reading process completed")
        return data

    def apply_filters(self, df, filters, df_name):

        """
        The apply_filters function takes a dataframe, a list of filters and the name of the dataframe as input.
        It then applies each filter to the dataframe and returns it.
        The function raises an exception if there is an error in applying any filter.
        :param df: Get the dataframe to apply the filters on
        :param filters: Pass in the filter rules to apply
        :param df_name: Identify the dataframe that is being filtered
        :return: A dataframe with the filters applied
        """
        try:
            query = pl.lit(True)

            for filter_rule in filters:
                col = filter_rule['column']
                operation = filter_rule['operation']
                values = filter_rule['values']
                operator = filter_rule.get('operator', 'and')

                # Validate operation
                if operation not in self.__available_operations:
                    msg = (f"{operation} is not allowed, the only allowed operations are "
                           f"'{', '.join(map(str, self.__available_operations))}'")
                    logger.exception(msg)
                    raise FilterConfigurationError(msg)

                # Validate values for certain operations
                if operation not in ('notin', 'in') and isinstance(values, list):
                    msg = (f"For list values, use 'notin' or 'in' operation. "
                           f"DataFrame name: {df_name}")
                    logger.exception(msg)
                    raise FilterConfigurationError(msg)

                # Build filter condition based on the operation
                condition = self._build_filter_condition(df, col, operation, values)

                # Combine conditions based on the specified logical operator
                if operator == 'and':
                    query = query & condition
                elif operator == 'or':
                    query = query | condition

            return df.filter(query)
        except Exception as e:
            logger.error(f"Error applying filters to {df_name}: {e}")
            raise FilterConfigurationError(f"Error in filter configuration for {df_name}: {e}")


    def _build_filter_condition(self, df, column, operation, values):
        """
        The _build_filter_condition function takes in a dataframe, column name, operation and values.
        It then builds a condition based on the operation and values provided.
        The function returns the condition.
        :param df: Specify the dataframe that we want to filter
        :param column: Specify the column name that we want to filter on
        :param operation: Determine which condition to use
        :param values: Create a condition based on the operation
        :return: A boolean series
        """
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

    def _read_from_directory(self, directory_path, join_similar, **kwargs):
        """
        The _read_from_directory function is a helper function that reads all the files in a directory and returns
        a list of dataframes. The function takes as input:
            - directory_path: the path to the directory containing all of our files.
            - join_similar: whether or not we want to join similar dataframes together (i.e., if they have identical columns).
                            This is useful for when we are reading from multiple directories, but want to combine them into one
                            large dataframe at some point later on in our code.
        :param directory_path: Specify the path to a directory containing files that will be read
        :param join_similar: Join the similar sentences
        :param **kwargs: Pass a variable number of keyword arguments to a function
        :return: The _read_files_parallel function
        """
        files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if
                 os.path.isfile(os.path.join(directory_path, f))]
        return self._read_files_parallel(files, join_similar, **kwargs)

    def _read_from_zip(self, zip_source, join_similar, **kwargs):
        """
        The _read_from_zip function is a helper function that reads in the zip file and returns
        the dataframe. It takes in the following parameters:
            - zip_source: The source of the zip file, either as a string or bytes object.
            - join_similar: A boolean value indicating whether to join similar columns together into one column.
                            If True, then all columns with similar names will be joined together into one column
                            (e.g., &quot;Name&quot; and &quot;NAME&quot; will become just &quot;Name&quot;). If False, then no joining occurs.
        :param zip_source: Specify the source of the zip file
        :param join_similar: Join similar files in the zip file
        :param **kwargs: Pass a variable number of keyword arguments to a function
        :return: A list of files
        """
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

        return self._read_files_parallel(files, join_similar, **kwargs)

    def _read_files_parallel(self, files, join_similar, **kwargs):
        """
        The _read_files_parallel function is a helper function that reads all the files in parallel.
        It uses ThreadPoolExecutor to create a pool of threads and map them to the _read_file function.
        The results are then stored in a dictionary with keys being df_{base_name} where base_name is
        the name of the file without any extensions or numbers at the end.

        :param files: Pass the list of files to be read
        :param join_similar: Join similar files into one dataframe
        :param **kwargs: Pass keyworded, variable-length argument list to a function
        :return: A dictionary of dataframes
        """
        with ThreadPoolExecutor() as executor:
            # Pass kwargs to the file reading function
            results = executor.map(lambda f: self._read_file(f, join_similar, **kwargs), files)

        dataframes = {}
        for file_name, df in results:
            if df is not None:
                logger.info(f"File reading for {file_name} finished")
                base_name = os.path.splitext(os.path.basename(file_name))[0]
                base_name = re.sub(r'_(\d+)|_(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})|_(\d{14})', '',
                                   base_name) if join_similar else base_name
                key = f'df_{base_name}'
                if key in dataframes and join_similar:
                    dataframes[key] = pl.concat([dataframes[key], df])
                else:
                    dataframes[key] = df

        return dataframes

    def _read_file(self, file, join_similar, **kwargs):
        """
        The _read_file function is a helper function that reads in the data from a file.
        It takes as input:
            - A file object (either an open zipfile or an open text/csv/parquet file)
            - A boolean indicating whether to join similar columns together (e.g., if there are two columns called 'x' and 'y',
                then they will be joined into one column called 'xy')

        :param file: Pass the file name to the function
        :param join_similar: Join similar columns in the dataframe
        :param **kwargs: Pass a dictionary of arguments to the function
        :return: A tuple of the file name and a dataframe
        """
        file_name = file.name if isinstance(file, zipfile.ZipExtFile) else file
        try:
            ext = os.path.splitext(file_name)[1]
            read_func = self.data_formats.get(ext)

            if not read_func:
                raise UnsupportedFormatError(f"Unsupported file format: {ext}")

            logger.info(f"Initiating reading of {file_name}")
            return file_name, read_func(file, **kwargs)
        except Exception as e:
            logger.error(f"Error reading file {file_name}: {e}")
            raise  # Re-raise other exceptions

