from pyspark.sql.functions import *
from pyspark.sql.types import *


def process_topics(df):
  return (
    df
      .withColumn('created_at', to_timestamp(col('created_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .distinct()
  )


def process_groups(df):
  return (
    df
      .withColumn('created_at', to_timestamp(col('created_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('updated_at', to_timestamp(col('updated_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('deleted', col('deleted').cast(BooleanType()))
      .withColumn('external', col('external').cast(BooleanType()))
      .withColumn('private', col('private').cast(BooleanType()))
      .distinct()
  )


def process_users(df):
  return (
    df
      .withColumn('joined_at', to_timestamp(col('joined_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('suspended_at', to_timestamp(col('suspended_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .distinct()
  )


def process_messages(df):
  """Some transformations have to be done for some columns to be usable:


  Notes
  _____
  Data exploration revealed no other tags than: user, opengraphobject and uploadedfile

  {
    attachments: "opengraphobject:[351561244736612 : https://www.eventbrite.ca], uploadedfile:137082854, opengraphobject:[351561246009480 : https://forms.office.com]",
    participants: "user:1489760410,user:1538539363"
  }

    =>

  {
    opengraph_objects: [
      { id: "351561244736612", url: "https://www.eventbrite.ca" },
      { id: "351561246009480", url: "https://forms.office.com" }
    ],
    uploaded_files: [ "137082854" ],
    participant_users: [ "1489760410", "1538539363" ]
  }
  """
  reg = '(opengraphobject:)|(\[.*\] )|(, opengraphobject:)|(uploadedfile:)|(, uploadedfile:)|(\[[^\[\]]+(?=\])\])'
  reg2 = '(opengraphobject:\[)|(\]*, opengraphobject:\[)|(uploadedfile:\w*)|(\]*, uploadedfile:\w*)|\]'
  fn = "transform(opengraph_objects, x -> named_struct( 'id', split(x, ' : ')[0], 'url', split(x, ' : ')[1] ))"
  opengraph_schema = ArrayType(
    StructType([StructField('id', StringType(), False), StructField('url', StringType(), False)]), False)
  return (
    df
      .withColumn('created_at', to_timestamp(col('created_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('deleted_at', to_timestamp(col('deleted_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('in_private_group', col('in_private_group').cast(BooleanType()))
      .withColumn('in_private_conversation', col('in_private_conversation').cast(BooleanType()))
      .withColumn('participant_users', array_remove(split(regexp_replace(col('participants'), 'user:', ''), ','), ''))
      .withColumn('uploaded_files',
                  when(col('attachments').like('%uploadedfile%'),
                       array_remove(split(col('attachments'), reg), ''))
                  .otherwise(from_json(lit('[]'), ArrayType(StringType()))))
      .withColumn('opengraph_objects',
                  when(col('attachments').like('%opengraphobject%'),
                       array_remove(split(col('attachments'), reg2), '')))
      .withColumn('opengraph_objects',
                  when(col('opengraph_objects').isNotNull(),
                       expr(fn))
                  .otherwise(from_json(lit('[]'), opengraph_schema)))
      .drop('gdpr_delete_url')
      .distinct()
  )


def process_messageslikes(df):
  """De-nesting stats struct since nested structure of literals offers no advantages.

  Parameters
  ----------
  df: pyspark.sql.DataFrame

  Returns
  -------
  pyspark.sql.DataFrame
  """
  return (
    df
      .select(col('messageId').alias('message_id'),
              explode(col('usersLikes')).alias('temp'))
      .select(col('message_id').cast(StringType()),
              col('temp.*'),
              col('temp.stats.followers').alias('followers'),
              col('temp.stats.following').alias('following'),
              col('temp.stats.updates').alias('updates'))
      .withColumn('activated_at',
                  to_timestamp(unix_timestamp(col('activated_at'), "yyyy/MM/dd HH:mm:ss").cast(TimestampType()),
                               "yyyy-mm-dd'T'HH:mm:ss"))
      .withColumn('id', col('id').cast(StringType()))
      .withColumn('network_id', col('network_id').cast(StringType()))
      .drop('stats')
      .drop('updatable_profile')  # Not always there, need to drop
      .distinct()
  )


curated_transforms = {
  'Users': process_users,
  'Topics': process_topics,
  'Groups': process_groups,
  'Messages': process_messages,
  'MessagesLikes': process_messageslikes
}
