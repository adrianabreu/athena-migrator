from os.path import dirname, abspath 
from pathlib import Path

class ParametrizedQuery():
    def __init__(self, query_file_name, params = None):
        self.query_file_name = query_file_name
        self.query = Path('data/scripts/' + query_file_name).absolute()
        self.params = params
    
    def get(self) -> str:
        with open(self.query) as query:
            q = query.read()
            if self.params:
                for key in self.params.keys():
                    q = q.replace(key, self.params[key])
            return q

def get_migrations(BUCKET: str):
    return [
        ParametrizedQuery('202206211901-create-db-refined.hql'),
        ParametrizedQuery('202206211935-create-table-test.hql', {'#BUCKET': BUCKET})
    ]


    