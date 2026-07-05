from fastapi import APIRouter
from schemas.vehicle import VehicleRequest, RCResponse
from services.rc_service import process_rc

router = APIRouter()

@router.post("/rc", response_model=RCResponse)
def get_rc(request: VehicleRequest):
    return process_rc(request)
