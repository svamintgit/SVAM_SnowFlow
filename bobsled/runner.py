from snowflake.snowpark import Session, Row
from snowflake.connector.errors import ProgrammingError, DatabaseError
import logging
from . import scripts
import os
import toml

class SnowflakeUser:
    def __init__(self, environment: str):
        if not environment:
            raise ValueError("Environment not specified. Please provide a valid environment.")
        self.environment = environment
        self.environment_obj = scripts.Environment(environment) 
        self.query_variables = self.environment_obj.query_variables
        self.config = self._load_toml_config()
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
        
    def _load_toml_config(self) -> dict:
        home_dir = os.path.expanduser("~") 
        toml_file_path = os.path.join(home_dir, ".snowflake", "connections.toml")

        try:
            with open(toml_file_path, "r") as toml_file:
                config = toml.load(toml_file)
                return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find connections.toml file at {toml_file_path}.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the TOML file: {e}")
        
    def _connect_with_rsa(self, environment_config):
        try:
            private_key_path = environment_config.get("private_key_path")

            if not os.path.isabs(private_key_path):
                private_key_path = os.path.join(os.getcwd(), private_key_path)

            if private_key_path:
                with open(private_key_path, "rb") as key_file:
                    private_key = key_file.read()

                passphrase = environment_config.get("private_key_passphrase")

                session_builder = Session.builder.configs({
                    "account": environment_config["account"],
                    "user": environment_config["user"],
                    "authenticator": "snowflake_jwt",  
                    "private_key": private_key,
                    "role": environment_config["role"],
                    "database": environment_config["database"],
                    "warehouse": environment_config["warehouse"],
                })

                if passphrase:
                    session_builder = session_builder.config("private_key_passphrase", passphrase)

                return session_builder.create()
            else:
                raise ValueError(f"Private key not found for environment '{self.environment}' in the TOML file.")
        except FileNotFoundError as e:
            logging.error(f"Private key file not found at path: {private_key_path}. Error: {e}")
            raise
        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake connection error: {db_error}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occured during RSA authentication: {e}")
            raise

    def _connect_with_password(self):
        try:
            session = Session.builder.config("connection_name", self.environment).create()
            return session
        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake connection error: {db_error}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occured during connection: {e}")
            raise

    def _get_session(self) -> Session:
        try:
            environment_config = self.config.get(self.environment)
            if not environment_config:
                raise ValueError(f"Environment '{self.environment}' not found in the TOML file.")

            logging.info(f"Loaded configuration for {self.environment}: {self.config}")

            if "private_key_path" in environment_config:
                return self._connect_with_rsa(environment_config)
            elif "user" in environment_config and "password" in environment_config:
                return self._connect_with_password()
            else:
                raise ValueError(f"Missing login credentials. Please add either 'private_key_path' for RSA key-pair authentication or 'user' and 'password' for username-password authentication to the TOML file.")

        except ValueError as ve:
            logging.error(f"ValueError: {ve}")
            raise
        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake connection error: {db_error}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            raise RuntimeError('Failed to establish Snowflake session')

    def run_query(self, query:str) -> list[Row]:
        try:
            if query.strip() != '':
                res = self.session.sql(query).collect()
            else:
                res = [Row()]    
            return res
        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake query execution error: {db_error}")
            logging.error(f"Query: {query}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during query execution: {e}")
            logging.error(f"Query: {query}")
            raise
    
    def run_queries(self, queries: list) -> list:
        '''
        executes a list of queries in order, returns a list of query output
        '''
        outp = []
        for query in queries:
            try:
                outp.append(self.run_query(query))
            except (ProgrammingError, DatabaseError) as db_error:
                logging.error(f"Programming query execution error: {db_error}")
                logging.error(f"Query: {query}")
                raise
            except Exception as e:
                logging.error(f"Unexpected error while executing query: {e}")
                logging.error(f"Query: {query}")
                raise
        return outp
    
    def post_files(self, file_configs: list[dict]) -> list:
        outp = []
        for file_config in file_configs:
            outp.append(self.session.file.put(file_config['local_path'], file_config['stage_path'], auto_compress=False, overwrite=True))
        return outp