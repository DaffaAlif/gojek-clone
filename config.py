import os
from dotenv import load_dotenv

_env = os.getenv('APP_ENV', 'development')

if _env == 'test':
    load_dotenv('.env.test', override=True)
else:
    load_dotenv('.env', override=True)

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
}

_prefix = os.getenv('KAFKA_TOPIC_PREFIX', '')

TOPICS = {
    'location_updates': f'{_prefix}location-updates',
    'ride_requests':    f'{_prefix}ride-requests',
    'ride_matched':     f'{_prefix}ride-matched',
    'ride_status':      f'{_prefix}ride-status',
    'ride_tracking':    f'{_prefix}ride-tracking',
    'eta_updates':      f'{_prefix}eta-updates',
}

JAKARTA_BOUNDS = {
    'lat_min': float(os.getenv('JAKARTA_LAT_MIN', '-6.3000')),
    'lat_max': float(os.getenv('JAKARTA_LAT_MAX', '-6.1000')),
    'lng_min': float(os.getenv('JAKARTA_LNG_MIN', '106.7500')),
    'lng_max': float(os.getenv('JAKARTA_LNG_MAX', '106.9500')),
}
