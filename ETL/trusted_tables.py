"""
This module provides utility functions to select/read/write JSON data from the trusted zone.

The names and structure of tables, folders and paths must conform to the library's standard convention:

  - trusted-zone -| - data_source - ...
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
from . import dataframe_utils, curated, curated_control
from .utils import build_path, df_empty
from .internal_utils import needs_params


@needs_params
def read(config, table, **dataframe_reader_options):
  """Read trusted zone's json files.

  Parameters
  ----------
  config: Config
    Config instance

  table: str

  dataframe_reader_options
  """
  table = config.validate_table_name(table)
  path = build_path(config, 'trusted', table)
  return dataframe_utils.read(path, **dataframe_reader_options)


def run_transformation(transformation, df, *args, **kwargs):
  """Run transformation on df using arguments.

  Pass a tuple like (transform, df, myarg, other_arg=val)

  Parameters
  ----------
  transformation: (pyspark.sql.DataFrame, args, kwargs) -> pyspark.sql.DataFrame
    A function taking a df and returning a df.

  df: pyspark.sql.DataFrame

  Returns
  -------
  pyspark.sql.DataFrame
  """
  if not callable(transformation):
    raise ValueError('First argument must be a function taking a df and returning a df')
  return transformation(df, *args, **kwargs)


@needs_params
def write(config, table, num_files=10, df=None, transformation=None,
          incremental=False, **dataframe_writer_options):
  """Writes the spark dataframe in json format to the trusted zone.

  Parameters
  ----------
  config: Config
    Config instance

  table: str

  num_files: int
    How many files to write

  df: pyspark.sql.DataFrame
    Dataframe to write. Leave None to read table from curated.

  transformation: (pyspark.sql.DataFrame) -> pyspark.sql.DataFrame, optional
    A function taking a df and returning a df.

  incremental: bool, optional
    To perform incremental load based on the curated-zone's control table content.
    Only possible when not passing your own DF.

  dataframe_writer_options
  """
  table = config.validate_table_name(table)
  if incremental is True and df is not None:
    raise ValueError('Incremental mode not possible when passing a dataframe.')

  # User passed his own df but its empty.
  if df is not None and df_empty(df):
    config.debug('An empty DF was supplied for table {}'.format(table), write)
    return

  df = df if df else curated.read(config, table, incremental=incremental)

  # User did NOT passed his own df but a table name to read from curated
  # but that resulted in an empty df.
  if df is None or df_empty(df):
    config.debug('Reading the raw data returned nothing for table {}'.format(table), write)
    return

  df = transformation(df) if transformation else df

  # The df is empty after applying the transformations.
  if df is None or df_empty(df):
    config.debug('Df is empty after applying the transformation for table {}'.format(table), write)
    return

  path = build_path(config, 'trusted', table)
  dataframe_utils.write(config, df, 'trusted', path, file_format='json', num_files=num_files,
                        timestampFormat="yyyy-MM-dd'T'HH:mm:ss", **dataframe_writer_options)
  curated_control.update(config, table)
