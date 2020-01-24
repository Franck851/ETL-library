"""
This module provides utility functions to select/read JSON data from the ingestion raw zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - raw-zone -| - data_source - ...
              |
              |
              | - data_source - ...
              |
              |
              | - yammer -  |
                            |
                            | - ingestion_date -| - table_name
                            |                   | - table_name
                            |
                            | - ingestion_date -| - table_name
                            |                   | - table_name
                            |
                            | - ingestion_date -| - table_name
                            |
                            |
                            | - inc-2019-12-31 -| - Users
                            |                   | - Messages
                            |
                            | - inc-2020-01-01 -| - Messages
                                                | - Groups


Data cannot be deleted/updated.
"""
from . import dataframe_utils, raw_control
from .config import Config
from .spark_init import get_spark_dbutils
from .internal_utils import needs_params


@needs_params
def read(config, table, incremental=False, **dataframe_reader_options):
  """

  Parameters
  ----------
  config: Config
  table
  incremental

  Returns
  -------
  (pyspark.sql.DataFrame, list)
  """
  table = config.validate_table_name(table)
  raw_zone_file_paths, control_table_updates = get_table_file_paths(config, table, incremental=incremental)
  df = dataframe_utils.read(config, 'raw', raw_zone_file_paths, **dataframe_reader_options)

  config.debug('Read {} new raw files for {} new rows for table {}:'.format(len(raw_zone_file_paths), df.count(), table), read)
  config.debug(raw_zone_file_paths, read)

  return df, control_table_updates


@needs_params
def get_table_file_paths(config, table, incremental=False):
  """Get all absolute paths of files with data for the provided table name across all incremental folders.

  Also returns short paths ready to update the raw control table.

  Return format:

  ( [table_file_paths, ...], [control_table_updates, ....] )

  (
    [
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json',
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json',
      '/dbfs/raw_mount/data_source/ingestion_date/table_name/some_file.json'
    ],
    [
      'inc-2019_09_08/table_name',
      'inc-2019_12_12/table_name'
    ]
  )

  Parameters
  ----------
  config: Config
  table
  incremental

  Returns
  -------
  (list of str, list of str)
  """
  _, dbutils = get_spark_dbutils()
  config.mount_zone('raw', force=False)
  table = config.validate_table_name(table)
  table_paths, control_table_updates = get_table_folder_paths(
    config, table, incremental=incremental)
  # Since glob.blob doesn't work from databricks connect, we cannot use wildcard patterns.
  file_paths = [y.path for x in table_paths for y in dbutils.fs.ls(x)]
  return file_paths, control_table_updates


@needs_params
def get_table_folder_paths(config, table, incremental=False):
  """Get all absolute paths of folders with data for the provided table name across all incremental folders.

  Also returns short paths ready to update the raw control table.
  Batching paths discovery by folders may also reduce chances of timeout.

  Return format:

  ( [table_folder_paths, ...], [control_table_updates, ....] )

  (
    [
      '/dbfs/raw_mount/data_source/inc-2019_09_08/table_name',
      '/dbfs/raw_mount/data_source/inc-2019_12_12/table_name
    ],
    [
      'inc-2019_09_08/table_name',
      'inc-2019_12_12/table_name'
    ]
  )

  Parameters
  ----------
  config: Config
  table
  incremental

  Returns
  -------
  (list of str, list of str)
  """
  _, dbutils = get_spark_dbutils()
  table = config.validate_table_name(table)
  config.mount_zone('raw', force=False)
  final_table_folder_paths, control_table_updates = [], []
  control_table_paths = raw_control.select(config, table)
  mount = config.get_mount_name_from_zone_name('raw')

  # Since glob.blob doesn't work from databricks connect,
  # we cannot use wildcard patterns.
  # (It would be: '/dbfs/mnt/adls/raw/data_source/*/table_name')
  inc_folders = [x.path for x in dbutils.fs.ls('{}/{}/'.format(mount, config.data_source))]
  table_folder_paths = [y.path for x in inc_folders for y in dbutils.fs.ls(x) if y.name[:-1] == table]

  for table_path in table_folder_paths:
    short_table_path = raw_control.path_to_short_path(table_path)
    in_control_table = any(short_table_path in control_table_path for control_table_path in control_table_paths)
    excluded = any(excluded_raw_path in table_path for excluded_raw_path in config.params['excluded_raw_paths'])

    if not excluded and (not incremental or not in_control_table):
      final_table_folder_paths.append(table_path)
      if not in_control_table:
        control_table_updates.append(short_table_path)

  return final_table_folder_paths, control_table_updates
