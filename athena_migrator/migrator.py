
from .migrations import get_migrations
from datetime import datetime
from .athena_client import AthenaClient

def run(client: AthenaClient, bucket: str) -> int:
    try:
        applied_migrations = [ s["Data"][0]["VarCharValue"] for s in client.run_query("select script_name from migrations.versions")['ResultSet']['Rows'][1:] ]
    except:
        create_migration_table(client, bucket)
        applied_migrations = []
    
    print(applied_migrations)
    migration_files = get_migrations(bucket)
    scripts_to_apply = [m for m in migration_files if m.query_file_name not in applied_migrations]

    print(f"Total scripts to apply: {len(scripts_to_apply)}")
    for script in scripts_to_apply:
        print(f"Applying: {len(scripts_to_apply)}")
        run_migration(client, script.get(), script.query_file_name)
    
    return len(scripts_to_apply)

def create_migration_table(client: AthenaClient, bucket: str):
    client.run_query("create database if not exists migrations")
    client.run_query("""create external table migrations.versions (
        script_name STRING,
        applied TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION 's3://#BUCKET/migrations'""".replace('#BUCKET', bucket))

def run_migration(client: AthenaClient, script: str, script_name: str ):
    multiple_sentences = script.split(';')
    for sentence in multiple_sentences:
        client.run_query(query=sentence)
    update_table(client, script_name)

def update_table(client: AthenaClient, script_name: str):
    expression = f'INSERT INTO migrations.versions (script_name, applied) VALUES( \'{script_name}\', timestamp \'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\')'
    print(expression)
    client.run_query(expression)
    