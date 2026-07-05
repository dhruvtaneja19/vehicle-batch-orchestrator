failed_records = []

def add_failed_record(vehicle_id, api_name, error):
    failed_records.append({
        "vehicle_id": vehicle_id,
        "api": api_name,
        "error": error,
    })

def get_failed_records():
    return failed_records
