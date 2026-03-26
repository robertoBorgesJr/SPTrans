catalog = "sptrans_catalog"
schema = "gtfs_data"
volume_path = f"dbfs:/Volumes/{catalog}/{schema}/landing_zone/"
gtfs_files = ["agency", "calendar", "fare_attributes", "fare_rules", "frequencies", 
              "routes", "shapes", "stop_times", "stops", "trips"]
