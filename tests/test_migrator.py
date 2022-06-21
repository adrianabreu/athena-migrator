import pytest
from unittest.mock import (MagicMock, patch)
from typing import Generator
from moto import mock_athena

from athena_migrator.athena_client import AthenaClient
from athena_migrator.migrator import run
from unittest.mock import patch

@mock_athena
@patch('athena_migrator.migrator.get_migrations')
def test_no_migration_needed_to_be_applied(mock_migrations):
    mock_migrations.return_value = []
    client = AthenaClient('eu-west')
    client.run_query = MagicMock(return_value=[])
    run(client, 'raw-local')

@mock_athena
@patch('athena_migrator.migrator.get_migrations')
def test_should_create_database_if_not_exists(mock_migrations):
    mock_migrations.return_value = []
    client = AthenaClient('eu-west')
    client.run_query = MagicMock(side_effect= [Exception(), [], []])
    run(client, 'raw-local')