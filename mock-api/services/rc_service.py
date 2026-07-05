from schemas.vehicle import VehicleRequest
import random

def process_rc(vehicle: VehicleRequest):
    if random.random() < 0.3:
        raise Exception("RC Service Down")
        
    return {
        "vehicle_id": vehicle.vehicle_id,
        "owner": "Demo Owner",
        "registration_status": "ACTIVE"
    }
