from ETL import Config, curated, trusted, Transform
from yammer_params import params
from yammer_curated_transforms import curated_transforms
from yammer_trusted_transforms import trusted_transforms

config = Config(params)

for table in config.tables:
  curated_transform = Transform(curated_transforms[table])
  curated.merge(config, table, transformation=curated_transform,
                incremental=True, raw_dataframe_reader_options={'format': 'json'})

for table in config.tables:
  trusted_transform = Transform(trusted_transforms[table], config=config) \
    if table == 'Messages' else Transform(trusted_transforms[table])
  trusted.write(config, table, transformation=trusted_transform, incremental=False)

