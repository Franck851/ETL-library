from pyspark.sql.functions import *
from pyspark.sql.types import *
from ETL.string_format import format_hour, format_week_days, format_phone, format_yes_no, format_zip


def process_transit_hours(df):
  return (
    df
      .withColumn('DAY_OF_WEEK_NAME_FR', format_week_days('DAY_OF_WEEK', 'fr'))
      .withColumn('DAY_OF_WEEK_NAME_EN', format_week_days('DAY_OF_WEEK', 'en'))
      .withColumn('OPEN_HOUR_1', format_hour('OPEN_HOUR_1'))
      .withColumn('CLOSE_HOUR_1', format_hour('CLOSE_HOUR_1'))
      .withColumn('OPEN_HOUR_2', format_hour('OPEN_HOUR_2'))
      .withColumn('CLOSE_HOUR_2', format_hour('CLOSE_HOUR_2'))
      .withColumn('OPEN_HOUR_3', format_hour('OPEN_HOUR_3'))
      .withColumn('CLOSE_HOUR_3', format_hour('CLOSE_HOUR_3'))
      .distinct()
  )


def process_transits(df):
  return (
    df
      .withColumn('EXCHANGE_OFFICE', format_yes_no('EXCHANGE_OFFICE'))
      .withColumn('HANDICAPPED_ACCESS', format_yes_no('HANDICAPPED_ACCESS'))
      .withColumn('PRINTER', format_yes_no('PRINTER'))
      .withColumn('OPEN_24H_7D', format_yes_no('OPEN_24H_7D'))
      .withColumn('ATM_COUNT', col('ATM_COUNT').cast(IntegerType()))
      .withColumn('POSTAL_CODE', format_zip('POSTAL_CODE'))
      .withColumn('FAX', format_phone('FAX'))
      .withColumn('PHONE_NUMBER', format_phone('PHONE'))
      .withColumn('PHONE_LINE', when(length(col('PHONE')) > 10, expr("substr(PHONE, -4)")))
      .drop('PHONE')
      .distinct()
  )


def process_atm_hours(df):
  return (
    df
      .withColumn('OPEN_HOUR_1', format_hour('OPEN_HOUR_1'))
      .withColumn('CLOSE_HOUR_1', format_hour('CLOSE_HOUR_1'))
      .withColumn('OPEN_HOUR_2', format_hour('OPEN_HOUR_2'))
      .withColumn('CLOSE_HOUR_2', format_hour('CLOSE_HOUR_2'))
      .withColumn('OPEN_HOUR_3', format_hour('OPEN_HOUR_3'))
      .withColumn('CLOSE_HOUR_3', format_hour('CLOSE_HOUR_3'))
      .withColumn('DAY_OF_WEEK_NAME_FR', format_week_days('DAY_OF_WEEK', 'fr'))
      .withColumn('DAY_OF_WEEK_NAME_EN', format_week_days('DAY_OF_WEEK', 'en'))
      .distinct()
  )


def process_atms(df):
  return (
    df
      .withColumn('EXCHANGE', format_yes_no('EXCHANGE'))
      .withColumn('HANDICAPPED_ACCESS', format_yes_no('HANDICAPPED_ACCESS'))
      .withColumn('PRINTER', format_yes_no('PRINTER'))
      .withColumn('OPEN_24H_7D', format_yes_no('OPEN_24H_7D'))
      .withColumn('WITHDRAW_ONLY', format_yes_no('WITHDRAW_ONLY'))
      .distinct()
  )


curated_transforms = {
  'ATM': process_atms,
  'ATM_HOURS': process_atm_hours,
  'TRANSIT': process_transits,
  'TRANSIT_HOURS': process_transit_hours
}
