params = {
  'data_source': 'transits',  # Once chosen, this must never be changed
  'tables': {
    'ATM': {
      'unique_key': 'ATM_ID',
      'trusted_incremental_mode': False
    },
    'ATM_HOURS': {
      'unique_key': 'HOURS_ID',
      'trusted_incremental_mode': False
    },
    'HOURS_TYPE': {
      'unique_key': 'ID',
      'trusted_incremental_mode': False
    },
    'TRANSIT': {
      'unique_key': 'ID',
      'trusted_incremental_mode': False
    },
    'TRANSIT_HOURS': {
      'unique_key': 'HOURS_ID',
      'trusted_incremental_mode': False
    },
    'TRANSIT_TYPE': {
      'unique_key': 'ID',
      'trusted_incremental_mode': False
    },
  }
}
