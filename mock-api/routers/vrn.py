from fastapi import APIRouter
from schemas.vehicle import VehicleRequest, VRNResponse
from services.vrn_service import process_vrn

router = APIRouter()

@router.post("/vrn", response_model=VRNResponse)
def get_vrn(request: VehicleRequest):
    return process_vrn(request)
