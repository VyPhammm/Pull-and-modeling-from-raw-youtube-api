from airflow import settings
from airflow.models import Variable

EXECUTION_ENVIRONMENT = Variable.get("execution_environment", "dev")
DBT_DIR = settings.DAGS_FOLDER + '/dbt_sipher'

default_args_for_dbt_operators = {
  'dir': DBT_DIR,
  'profiles_dir': DBT_DIR,
  'target': EXECUTION_ENVIRONMENT
}
