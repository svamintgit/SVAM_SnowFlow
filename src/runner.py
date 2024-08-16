from snowflake.snowpark import Session, Row
import logging
import sys
import scripts

class SnowflakeUser:
    def __init__(self):
        self.environment = scripts.Environment()
        self.query_variables = self.environment.query_variables
        self.connection_keys = {
                "account": 'SNOWSQL_ACCOUNT',
                "user":'SNOWSQL_USER',
                "password": 'SNOWSQL_PWD',
                "warehouse": 'SNOWSQL_WAREHOUSE',
                "role": 'SNOWSQL_ROLE',
                "database":'SNOWSQL_DB'
            }
        self.session = self._get_session()
    
    def _get_connection_parameters(self, input_dict: dict) -> dict:
        connection_parameters =dict()
        logging.info(set(input_dict.keys()))
        logging.info(set(self.connection_keys.values()))
        if set(self.connection_keys.values()).issubset(set(input_dict.keys())):
            for input_key, lookup_key in self.connection_keys.items():
                connection_parameters[input_key]= input_dict[lookup_key]
            return connection_parameters
        else:
            logging.error('could not find all requirement connection params')
            return {}
    
    def _get_session(self) -> Session:
        try:
            connection_params = self._get_connection_parameters(self.environment.params)
            return Session.builder.configs(connection_params).create()
        except Exception as e:
            logging.error(e)
            logging.error('If using local_connection file, ensure parameters are correct. Else set your env variables')
            sys.exit(1)

    def run_query(self, query:str) -> list[Row]:
        try:
            if query.strip() != '':
                res = self.session.sql(query).collect()
            else:
                res = [Row()]    
            return res
        except Exception as e:
            logging.error(e)
            logging.error(query)
            return [Row()]
    
    def run_queries(self, queries: list) -> list:
        '''
        executes a list of queries in order, returns a list of query output
        '''
        outp = []
        for query in queries:
            outp.append(self.run_query(query))
        return outp
    
    def post_files(self, file_configs: list[dict]) -> list:
        outp = []
        for file_config in file_configs:
            outp.append(self.session.file.put(file_config['local_path'], file_config['stage_path'], auto_compress=False, overwrite=True))
        return outp

if __name__=="__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
    queries = ['select 1', 'select 2']
    session = Session.builder.config("connection_name", "DEV_ACCOUNT").create()
    print(session.sql('select 1'))