from . import curated_control, curated_tables, raw_control
from .utils import log
from .dbfs_utils import table_exists
from .spark_init import get_spark_dbutils
from .exceptions import NoTablesDefinedException


def delete(config, zones=None, tables=None, hard=False, check_tables=True):
  """Delete tables and control tables data at every zones.

  Parameters
  ----------
  config: Config
  zones: list or str, optional
  tables: list or str, optional
  hard: bool, optional
    Rarely any point in deleting the table itself. Simply delete contents.
    May be needed after many tests resulting in a ballooning delta table size.
  check_tables: bool, optional
    If false, can delete folders / tables with different names than in params.
  """
  spark, dbutils = get_spark_dbutils()
  zones = config.validate_zone_names(zones)
  if check_tables:
    tables = config.validate_table_names(tables)
    if len(tables) == 0:
      raise NoTablesDefinedException(delete)

  if hard:
    log('Deleting with hard mode. There is no going back...')

  if 'raw' in zones and table_exists(config.raw_control_table_name, config.data_source):
    log('Not deleting raw zone files, only control table content.')
    if hard:
      spark.sql('DROP TABLE IF EXISTS {}.{}'.format(
        config.data_source, config.raw_control_table_name))
    else:
      for table in tables:
        raw_control.delete(config, table)

  if 'curated' in zones:
    log('Deleting curated zone data and control table content...')
    if hard:
      spark.sql('DROP TABLE IF EXISTS {}.{}'.format(
        config.data_source, config.curated_control_table_name))
    else:
      for table in tables:
        curated_tables.delete(config, table, drop=hard)
        curated_control.delete(config, table)

  if 'trusted' in zones and hard:
    log('Deleting trusted zone data...')
    mount = config.get_mount_name_from_zone_name('trusted')
    for table in tables:
      path = '{}{}/{}'.format(mount, config.data_source, table)
      dbutils.fs.rm(path, True)

  log('Done.')
