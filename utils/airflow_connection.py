def get_vehicle_api_url():
    try:
        # pyrefly: ignore [missing-import]
        from airflow.sdk.bases.hook import BaseHook
        connection = BaseHook.get_connection("vehicle_api")
    except Exception:
        # Fallback: query DB directly (works outside task context, e.g. Python REPL)
        # pyrefly: ignore [missing-import]
        from airflow.models import Connection
        # pyrefly: ignore [missing-import]
        from airflow.utils.session import create_session
        with create_session() as session:
            connection = session.query(Connection).filter(
                Connection.conn_id == "vehicle_api"
            ).first()

    return (
        f"{connection.schema}://"
        f"{connection.host}:"
        f"{connection.port}/api"
    )
