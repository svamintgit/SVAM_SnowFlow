from snowflake.snowpark import Session, Row
from snowflake.connector.errors import ProgrammingError, DatabaseError
import logging
import platform
import os
import toml
import sys

class ConnectionFile:
    def __init__(self, environment: str):
        self.environment=environment
        self.config_path = self.get_config_path()
        logging.info(f'opening {self.config_path}')
        self.config: dict = self.get_config_dict(self.config_path)
        self.required_fields = ["account", "role", "warehouse"]
        self.validate_config_keys()

    def get_config_path(self) -> str:
        system = platform.system() 
        user_root = os.path.expanduser('~')
        default_path = os.path.join(user_root, '.snowflake', 'connections.toml')

        if os.path.exists(default_path):
            return default_path
        elif system == "Windows":
            return self.get_windows_config_path()
        elif system == "Linux":
            return self.get_linux_config_path()
        elif system == "Darwin":
            return self.get_mac_config_path()
        else:
            logging.error(f"Operating system value not found. Expected values of Windows,Linux, or Darwin for platform.system() Found:"+system)
            raise
        
    def get_windows_config_path(self) -> str:
        try:
            base = os.path.expandvars('%USERPROFILE%')
            return os.path.join(base,'AppData','Local','snowflake', 'connections.toml')
        except Exception as e:
            logging.error(f"Unexpected error when reading Snowflake config file on Windows OS: {e}")
            raise
    
    def get_linux_config_path(self) -> str:
        try:
            base_path= os.path.expanduser('~')
            return os.path.join(base_path,'.config','snowflake','connections.toml')
        except Exception as e:
            logging.error(f"Unexpected error when reading Snowflake config file on Linux OS: {e}")
            raise

    def get_mac_config_path(self) -> str:
        try:
            base_path= os.path.expanduser('~')
            return os.path.join(base_path,'Library','Application Support','snowflake','connections.toml')
        except Exception as e:
            logging.error(f"Unexpected error when reading Snowflake config file on Linux OS: {e}")
            raise

    def get_config_dict(self, toml_path) -> dict:
        try:
            with open(toml_path, "r") as toml_file:
                config = toml.load(toml_file)
                return config[self.environment]
        except toml.TomlDecodeError as tde:
            raise  toml.TomlDecodeError(f'Error parsing the connections.toml file: {tde}')
        except FileNotFoundError:
            raise FileNotFoundError(f"Could not find connections.toml file at {toml_path}.")
        except Exception as e:
            raise Exception(f"An error occurred while reading the TOML file: {e}")
        except KeyError:
            raise KeyError(f"Could not find environment key {self.environment} in connections.toml file")

    def validate_config_keys(self) -> bool:
        if not set(self.required_fields) <= set(self.config.keys()):
            logging.error(f'Could not find all required connection parameters in environment {self.environment} in connection file {self.config_path}')
            raise

class SnowflakeUser:
    def __init__(self, environment: str):
        if not environment:
            raise ValueError("Environment not specified. Please provide a valid environment.")
        self.environment = environment
        self.connection_file = ConnectionFile(self.environment)
        self.connection_config = self.connection_file.config
        self.session = self._get_session()
        
    def _connect_with_rsa(self):
        try:
            private_key_path = self.connection_config.get("private_key_path")

            if not os.path.isabs(private_key_path):
                private_key_path = os.path.join(os.getcwd(), private_key_path)

            if private_key_path:
                with open(private_key_path, "rb") as key_file:
                    private_key = key_file.read()
                self.connection_config['private_key']= private_key
                self.connection_config['authenticator']= "snowflake_jwt"
                session_builder = Session.builder.configs(self.connection_config)
                return session_builder.create()
            else:
                raise ValueError(f"Private key not found for environment '{self.environment}' in the TOML file. Looked in path {private_key_path}")
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

    def _connect_with_sso(self):
        try:
            session = Session.builder.config("connection_name", self.environment).create()

            return session

        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake connection error: {db_error}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred during SSO authentication: {e}")
            raise

    def _get_session(self) -> Session:
        try:
            if self.connection_config.get("private_key_path"):
                return self._connect_with_rsa()
            elif self.connection_config.get("authenticator") == "externalbrowser":
                return self._connect_with_sso()
            elif self.connection_config.get("user") and self.connection_config.get("password"):
                return self._connect_with_password()
            else:
                raise ValueError(f"Missing login credentials. Please add either 'private_key_path' for RSA key-pair authentication or 'user' and 'password' for username-password authentication, or authenticator: externalbrowser for SSO authentication")
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
            logging.error(f"Error during query execution: {e}. Query: {query}. Skipping query execution.")
    
    def run_queries(self, queries: list, object_type: str = "") -> list:
        """
        Executes a list of queries in order and returns a list of output results.
        Logs and skips execution if no queries are provided.
        """
        outp = []

        for index, query in enumerate(queries):
            try:
                logging.info(f"Executing query {index + 1}/{len(queries)} for {object_type}")
                logging.debug(f"Executing query {index + 1}/{len(queries)} for {object_type}: {query}")
                result = self.run_query(query)
                outp.append(result)
                logging.debug(f"Query executed successfully: {result}")
            except (ProgrammingError, DatabaseError) as db_error:
                logging.error(f"Database error in {object_type} query execution: {db_error}")
                logging.error(f"Query: {query}")
                raise
            except Exception as e:
                logging.error(f"Unexpected error in {object_type} query execution: {e}. Query: {query}. Skipping query.")
    
        logging.debug(f"Completed all queries for {object_type}, with {len(outp)} successful executions.")
        return outp
    
    def post_files(self, file_configs: list[dict]) -> list:
        outp = []
        for file_config in file_configs:
            logging.info(f"Loading local file {file_config['local_path']} to {file_config['stage_path']}")
            outp.append(self.session.file.put(file_config['local_path'], file_config['stage_path'], auto_compress=False, overwrite=True))
        return outp
    
if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')