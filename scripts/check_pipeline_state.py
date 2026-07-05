from airflow.exceptions import AirflowFailException
from airflow.models import Variable

from utils.logger import get_logger

logger = get_logger(__name__)


def check_pipeline_state():

    state = Variable.get(
        "PIPELINE_STATE",
        default_var="RUNNING"
    )

    logger.info(f"Pipeline State : {state}")

    if state == "RUNNING":
        return

    if state == "PAUSED":
        raise AirflowFailException(
            "Pipeline is paused."
        )

    if state == "CANCELLED":
        raise AirflowFailException(
            "Pipeline has been cancelled."
        )