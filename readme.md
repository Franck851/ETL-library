Documentation: https://charlesdarkwind.github.io/ETL-lib.github.io/html/

For development or analysis purposes, the library saves time by never mounting volumes unless necessary
or if asked to mount anyway, even after clearing a notebook's state. </br>
The volumes accessibility is always assessed at the last possible moment to reduce the chances of using
outdated credentials.

Tables data location is abstracted away.
___
<p>&nbsp;</p>

##### Standard / low abstraction modules


* raw
* curated
* trusted
* raw_control
* curated_control

These modules contain utility functions for read, write and delete operations while enforcing conventional naming, file and folder locations and other standards to keep things organised.


These also log a lot of debug informations in a log file located at:</br>
* windows: C://logs
* data lake: raw-zone/logs
___
<p>&nbsp;</p>

##### Utility modules
* utils
* config
* dbfs_utils
* json_utils
* delta_utils

For more flexibility in order to build pipelines that can cover many other use cases.</br>
The higher-order / standard modules seen before all implement functions from these utility modules.

<b>utils</b> is the only module that can be imported, and its functions used, from anywhere without needing spark or a databricks connection.
___
<p>&nbsp;</p>

#### Installation
```shell script
pip install ETL-lib
```
```python
dbutils.library.installPyPI('ETL-lib')
```
#### Examples

```python
from pyspark.sql.functions import to_timestamp, col
from pyspark.sql.types import TimestampType

from yammer_params import params
from ETL import *

config = Config(params)


def parse_date(df):
  return df.withColumn(
    'created_at', to_timestamp(col('created_at').cast(TimestampType()), "yyyy-mm-dd'T'HH:mm:ss"))


# Read all raw-zone data for table "Messages" and overwrite the curated-zone delta table:
curated.write(config, 'Messages', transformation=parse_date)

# Read only new raw-zone folders and merge it into the curated-zone:
curated.merge(config, 'Messages', transformation=parse_date, incremental=True)

# Since raw can only go to curated, ETL.curated_tables.merge() and ETL.curated.write() do it implicitly


# Example of the same merge but more explicitly using other functions from the library
def raw_to_curated(table, transformation=None, incremental=True):

  # Read raw data, also retrieve potential control table updates
  df, short_paths = raw.read(config, table, incremental=incremental)

  # Clean it
  if transformation and not utils.df_empty(df):
    df = transformation(df)

  # Merge into curated table
  curated.merge(config, table, df, incremental=incremental)

  # Update control table
  raw_control.insert(config, short_paths)
```