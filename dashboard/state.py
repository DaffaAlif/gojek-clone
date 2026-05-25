import threading

lock = threading.Lock()

# From location_updates
drivers_data = {}     # driver_id → latest location payload

# From ride_matched
matches_data = []     # list of recent match events (newest first)
active_match = {}     # driver_id → match payload {pickup, destination, ...}

# From ride_tracking
tracking_data = {}    # driver_id → latest tracking payload
driver_trail  = {}    # driver_id → [[lat, lng], ...]

# From ride_status
ride_status = {}      # driver_id → status string

# From eta_updates
eta_data = {}         # driver_id → latest ETA payload

thread_started = False

# Health: when each Kafka topic last delivered a message (float timestamp)
node_last_seen = {}   # topic string → float
