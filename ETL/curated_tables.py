"""
This module provides utility functions to select, insert and delete operations on delta tables from the curated zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - curated-zone -| - data_source - ...
                  |
                  |
                  | - data_source - ...
                  |
                  |
                  | - yammer -  | - table_name
                                | - table_name
                                | - Messages
                                | - Users
                                | - Likes

"""
from . import raw_tables, curated_control, delta_utils, raw_control
from .config import Config
from .utils import build_path, df_empty
from .internal_utils import needs_params


@needs_params
def read(config, table, incremental=False, **raw_dataframe_reader_options):
  """Read all data, equivalent of doing a "SELECT *"

  Returns None if no data at path or path does not exists.
  Can still return a empty dataframe or a dataframe of empty rows if such is obtained from reading the files.

  Parameters
  ----------
  config: Config
  table: str
  incremental: bool
    To perform incremental read based on the raw-zone's control table content.
  raw_dataframe_reader_options: dict, optional
    If df is not None, options are used when reading raw
  raw_dataframe_reader_options

  Returns
  -------
  pyspark.sql.DataFrame
  """
  table = config.validate_table_name(table)
  path = build_path(config, 'curated', table)
  full_df = delta_utils.read(config, 'curated', path, **raw_dataframe_reader_options)
  old_version = curated_control.select(config, table)

  if not incremental or not old_version:
    config.debug('Returning full curated data dataframe ({} rows) for table {}'.format(full_df.count(), table), read)
    return full_df

  config.debug('Returning the incremental extract of curated data updates '
               'since last run for table {}'.format(table), read)

  # DF from the last time it was written in the trusted zone
  old_df = delta_utils.read(
    config, 'curated', path, versionAsOf=old_version, **raw_dataframe_reader_options)

  updates = full_df.subtract(old_df)

  config.debug('Read {} new curated rows for table {}'.format(updates.count(), table), read)

  return updates


@needs_params
def write(config, table, df=None, transformation=None, incremental=False,
          raw_dataframe_reader_options=None, **dataframe_writer_options):
  """Write/save data in the delta table.

  Parameters
  ----------
  config: Config
    Config instance
  table: str
    Name of the table / folder.
  df: pyspark.sql.DataFrame, optional
    Dataframe containing the data to save in the curated delta table.
  transformation: (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.
  incremental: bool, optional
    To perform incremental load based on the raw-zone's control table content.
  raw_dataframe_reader_options: dict, optional
    If df is not None, options are used when reading raw zone files.
  dataframe_writer_options:
      mode: str
          'overwrite' or 'append'
  """
  table = config.validate_table_name(table)
  if incremental is True and df is not None:
    raise ValueError('Incremental mode not possible when passing a dataframe.')

  # User passed his own df but its empty.
  if df is not None and df_empty(df):
    config.debug('An empty DF was supplied for table {}'.format(table), write)
    return

  df, short_paths = (df, None) if df \
    else raw_tables.read(config, table, incremental=incremental, **raw_dataframe_reader_options)

  # User did NOT pass his own df but a table name to read from raw
  # but that resulted in an empty df.
  if df is None or df_empty(df):
    config.debug('Reading the raw data returned nothing for table {}'.format(table), write)
    return

  df = transformation(df) if transformation else df

  # The df is empty after applying the transformations.
  if df is None or df_empty(df):
    config.debug('Df is empty after applying the transformation for table {}'.format(table), write)
    return

  path = build_path(config, 'curated', table)
  delta_utils.write(config, df, 'curated', table, path, **dataframe_writer_options)
  raw_control.insert(config, short_paths)


@needs_params
def merge(config, table, df=None, transformation=None, unique_key=None,
          incremental=False, raw_dataframe_reader_options=None):
  """Update data in the delta table.

  If df is not provided, read table data from raw.

  Performs a 'merge into'/'upsert' of a dataframe/table-view inside the Delta Table.
  Meaning rows are overwritten if it finds one with the same designated unqique fields, or else it is inserted.
  The batch of updates must not contain more than one row with the same chosen unique key.

  Handles the creation of the SQL condition update part. Needs an unique key, can be a combination of 2 or more fields.
  Eg. "ON delta_messages.message_id = updates.message_id AND delta_messages.id = updates.id"

  Parameters
  ----------
  config: Config
    Config instance
  table: str
    Name of the table / folder.
  df: pyspark.sql.DataFrame, optional
    Dataframe containing the data to save in the curated delta table.
  transformation: (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.
  unique_key: list or str, optional
    Columns with unique values or list of columns to use as a combined unique key.
  incremental: bool, optional
    To perform incremental load based on the raw-zone's control table content.
  raw_dataframe_reader_options: dict, optional
    If df is not None, options are used when reading raw zone files.
  """
  if raw_dataframe_reader_options is None:
    raw_dataframe_reader_options = {}
  table = config.validate_table_name(table)
  if incremental is True and df is not None:
    raise ValueError('Incremental mode not possible when passing a dataframe.')

  # User passed his own df but its empty.
  if df is not None and df_empty(df):
    config.debug('An empty DF was supplied for table {}'.format(table), merge)
    return

  # User did NOT pass his own df but a table name to read from curated
  # but that resulted in an empty df.
  df, short_paths = (df, None) if df \
    else raw_tables.read(config, table, incremental=incremental, **raw_dataframe_reader_options)
  if df is None or df_empty(df):
    config.debug('Reading the raw data returned nothing for table {}'.format(table), write)
    return

  df = transformation(df) if transformation else df

  # The df is empty after applying the transformations.
  if df is None or df_empty(df):
    config.debug('Df is empty after applying the transformation for table {}'.format(table), write)
    return

  unique_key = unique_key or config.params['tables'][table]['unique_key']
  path = build_path(config, 'curated', table)
  delta_utils.merge(config, 'curated', table, df, path, unique_key)
  raw_control.insert(config, short_paths)


@needs_params
def delete(config, table, drop=False):
  """Delete delta data.

  Only use drop when really needed, otherwise just overwrite or delete.

  Parameters
  ----------
  config: Config
    Config instance
  table: str or list, optional
    Table name
    Leave empty to use all tables.
  drop: bool, optional
    Drop table
  """
  table = config.validate_table_name(table)
  path = build_path(config, 'curated', table)
  delta_utils.delete(config, 'curated', table, path, drop=drop)


upsert = merge
