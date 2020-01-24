from pyspark.sql.functions import *
from pyspark.sql.types import *
from ETL import get_spark_dbutils, curated
from ETL.dataframe_utils import groupby_and_to_list
from ETL.string_format import format_transit_id


def process_transits(df, config):
  spark, _ = get_spark_dbutils()

  hours_type = curated.read(config, 'HOURS_TYPE')

  transit_hours = (
    curated
      .read(config, 'TRANSIT_HOURS')
      .join(hours_type, col('HOURS_TYPE_ID') == hours_type.ID, 'left').drop('ID')
      .withColumnRenamed('NAME_FR', 'HOURS_TYPE_FR')
      .withColumnRenamed('NAME_EN', 'HOURS_TYPE_EN')
  )

  transit_hours = groupby_and_to_list(transit_hours, 'TRANSIT_ID', 'HOURS')

  transit_types = (
    curated
      .read(config, 'TRANSIT_TYPE')
      .withColumnRenamed('ID', 'TRANSIT_TYPE_ID')
      .withColumnRenamed('DESC_FR', 'TRANSIT_TYPE_FR')
      .withColumnRenamed('DESC_EN', 'TRANSIT_TYPE_EN')
  )

  return (
    df
      .join(transit_types, 'TRANSIT_TYPE_ID', 'left')
      .join(transit_hours, col('ID') == transit_hours.TRANSIT_ID, 'left').drop('TRANSIT_ID')
      .withColumn('DISPLAY_NAME_WITH_ID_FR', expr("concat(DISPLAY_NAME_FR, ' ', ID)"))
      .withColumn('DISPLAY_NAME_WITH_ID_EN', expr("concat(DISPLAY_NAME_EN, ' ', ID)"))
      .withColumn('ID', format_transit_id('ID'))
  )


def process_atms(df, config):
  spark, _ = get_spark_dbutils()

  hours_type = curated.read(config, 'HOURS_TYPE')

  atm_hours = (
    curated
      .read(config, 'ATM_HOURS')
      .join(hours_type, col('HOURS_TYPE_ID') == hours_type.ID, 'left').drop('ID')
      .withColumnRenamed('NAME_FR', 'HOURS_TYPE_FR')
      .withColumnRenamed('NAME_EN', 'HOURS_TYPE_EN')
  )

  atm_hours = groupby_and_to_list(atm_hours, 'ATM_ID', 'HOURS')

  return df.join(atm_hours, 'ATM_ID', 'left')
