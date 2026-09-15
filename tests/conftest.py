import pytest
from databricks.connect import DatabricksSession


@pytest.fixture(scope="session")
def spark():
    """Spark session on Databricks serverless, via Databricks Connect (uses ~/.databrickscfg DEFAULT profile)."""
    return DatabricksSession.builder.serverless(True).getOrCreate()
