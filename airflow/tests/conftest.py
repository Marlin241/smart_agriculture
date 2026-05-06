import os
import sys
from unittest.mock import MagicMock

for mod_name in [
    'airflow',
    'airflow.models',
    'airflow.operators',
    'airflow.operators.python',
    'airflow.providers',
    'airflow.providers.docker',
    'airflow.providers.docker.operators',
    'airflow.providers.docker.operators.docker',
    'airflow.exceptions',
    'docker',
    'docker.types',
]:
    sys.modules[mod_name] = MagicMock()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'dags')))
