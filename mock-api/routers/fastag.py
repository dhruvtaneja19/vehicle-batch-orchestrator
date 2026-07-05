from fastapi import APIRouter
from schemas.vehicle import VehicleRequest, FastagResponse
from services.fastag_service import process_fastag

router = APIRouter()

@router.post("/fastag", response_model=FastagResponse)
def get_fastag(request: VehicleRequest):
    return process_fastag(request)
