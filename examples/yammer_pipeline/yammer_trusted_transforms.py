from pyspark.sql.functions import *
from pyspark.sql.types import *
from ETL import exists, utils, get_spark_dbutils


def remove_empty_strings(df):
  return df.replace('', None)


def join_semaphores(df, mount, data_source, id_col, group_by_id):
  spark, _ = get_spark_dbutils()

  schema = ArrayType(StructType([
    StructField('score', StringType(), False),
    StructField('subject', StringType(), False),
    StructField('uri', StringType(), False)
  ]), False)

  path = mount + '/' + data_source + '/semaphore/control_file.json'
  if not exists(path):
    return df

  # Ensure that the file is not in use
  while utils.has_handle('/dbfs' + path):
    time.sleep(0.5)

  sem = (
    spark
      .read
      .json(path)
      .select(col('semaphore'),
              col(id_col).alias(group_by_id))
      .withColumn('sem_flag', lit(True))
  )

  return (
    df
      .drop('semaphore')
      .drop('sem_flag')
      .join(sem, group_by_id, 'left')
      .withColumn('semaphore',
                  when(col('semaphore').isNull(), from_json(lit('[]'), schema)).otherwise(col('semaphore')))
      .withColumn('sem_flag', when(col('sem_flag').isNull(), lit(False)).otherwise(col('sem_flag')))
  )


def process_messages(df, config):
  df = join_semaphores(df, config.get_mount_name_from_zone_name('trusted'), config.data_source, 'id', 'thread_id')
  return remove_empty_strings(df)


trusted_transforms = {
  'Messages': process_messages,
  'Users': remove_empty_strings,
  'Topics': remove_empty_strings,
  'Groups': remove_empty_strings,
  'MessagesLikes': remove_empty_strings
}
