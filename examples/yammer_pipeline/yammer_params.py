params = {
  'data_source': 'yammer',  # Once chosen, this must never be changed
  'excluded_raw_paths': [
    'inc-2019_10_01/MessagesLikes'
  ],
  'tables': {
    'MessagesLikes': {
      'unique_key': ['id', 'message_id'],
      'trusted_incremental_mode': False
    },
    'Messages': {
      'unique_key': 'id',
      'trusted_incremental_mode': False
    },
    'Users': {
      'unique_key': 'id',
      'trusted_incremental_mode': True
    },
    'Groups': {
      'unique_key': 'id',
      'trusted_incremental_mode': False
    },
    'Topics': {
      'unique_key': 'id',
      'trusted_incremental_mode': False
    }
  }
}
