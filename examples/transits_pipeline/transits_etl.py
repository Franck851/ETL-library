from ETL import Config, curated, trusted, Transform
from transit_params import params
from transit_curated_transforms import curated_transforms
from transit_trusted_transforms import process_transits, process_atms

config = Config(params)

raw_files_config = {
  'format': 'csv',
  'sep': ';',
  'header': True,
  'nullValue': '(null)',
  'encoding': 'ISO-8859-1'
}

for table in config.tables:
  curated_transform = Transform(curated_transforms[table]) if table in curated_transforms else None
  curated.merge(config, table, transformation=curated_transform, raw_dataframe_reader_options=raw_files_config)

trusted.write(config, 'TRANSIT', num_files=3, transformation=Transform(process_transits, config))
trusted.write(config, 'ATM', num_files=3, transformation=Transform(process_atms, config))


