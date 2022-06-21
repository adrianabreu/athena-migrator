from athena_migrator.migrations import get_migrations

def test_scripts_replace():
    scripts = get_migrations('raw-local')

    assert(scripts[0].get() == 'CREATE DATABASE IF NOT EXISTS db_refined;')
