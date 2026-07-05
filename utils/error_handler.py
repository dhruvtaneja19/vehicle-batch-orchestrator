from utils.logger import get_logger
from scripts.failed_batches import add_failed_record

logger = get_logger(__name__)

def handle_api_error(api_name: str, vehicle_id: str, error: Exception):
    logger.error(f"{api_name} failed for Vehicle {vehicle_id}: {str(error)}")
    
    add_failed_record(
        vehicle_id,
        api_name,
        str(error)
    )

    return {
        "status": "FAILED",
        "error": str(error),
    }
