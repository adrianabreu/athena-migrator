import boto3
from typing import Optional
from time import sleep

class AthenaClient():

    def __init__(self, region_name: str, sleep_time: int = 10):
        self.client = boto3.client(service_name = 'athena', region_name = region_name)
        self.sleep_time = sleep_time
        
    def run_query(self, query: str, database: str = 'default'):
        execution_id = self.start_query_execution(database, query)        
        while True:
            query_state = self.poll_query_status(execution_id)

            if query_state is None:
                print(f"Waiting for query completion, {execution_id}")
            elif query_state in ('QUEUED', 'RUNNING'):
                print(f"Query still running: {execution_id}")
            elif query_state in ('FAILED'):
                raise Exception('Query failed')
            else:
                print(f"Trial %s: Query execution completed. Final state is {query_state}", )
                break
            sleep(self.sleep_time)
        return self.client.get_query_results(QueryExecutionId=execution_id)


    def start_query_execution(self, database: str, query: str):
        response = self.client.start_query_execution(
            QueryString=query,
            QueryExecutionContext = {
                'Database': database,
                'Catalog': 'AwsDataCatalog'
            }
        )

        query_execution_id = response['QueryExecutionId']
        return query_execution_id

    def poll_query_status(self, query_execution_id: str) -> Optional[str]:
        response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
        state = None
        try:
            state = response['QueryExecution']['Status']['State']
        except Exception as ex:  # pylint: disable=broad-except
           print (f'Exception while getting query state {ex}')
        finally:
            return state  # pylint: disable=lost-exception

    