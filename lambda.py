from athena_migrator.migrator import run
from athena_migrator.config import (BUCKET, REGION)
from athena_migrator.athena_client import AthenaClient

def lambda_handler(event, context):
    client = AthenaClient(REGION)
    run(client, BUCKET) 
