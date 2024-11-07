from snowflake.snowpark import Session, Row
from snowflake.connector.errors import ProgrammingError, DatabaseError
import logging
from . import scripts
import os
import toml
import time
import json

class SnowflakeUser:
    def __init__(self, environment: str):
        if not environment:
            raise ValueError("Environment not specified. Please provide a valid environment.")
        self.environment = environment
        self.environment_obj = scripts.Environment(environment) 
        self.query_variables = self.environment_obj.query_variables or {}
        self.config = self._load_toml_config()
        self._validate_connection_parameters()
        self.session = self._get_session()
        self.token_cache_file = os.path.join(os.path.expanduser("~"), ".snowflake", "token_cache.json")

    def _validate_connection_parameters(self):
        """
        Check for listed connection parameters and warn if database is missing
        """

        environment_config = self.config.get(self.environment, {})

        required_fields = ["account", "role", "warehouse"]
        missing_fields = [field for field in required_fields if not environment_config.get(field)]
        
        if not environment_config.get("database"):
            logging.warning("No 'database' specified in the connection.")
        else:
            required_fields.append("database")

        if missing_fields:
            missing_str = ", ".join(missing_fields)
            raise ValueError(f"Missing required connection parameters: {missing_str}")
    
    def _get_connection_parameters(self, input_dict: dict) -> dict:
        connection_parameters =dict()
        logging.info(set(input_dict.keys()))
        logging.info(set(self.connection_keys.values()))
        if set(self.connection_keys.values()).issubset(set(input_dict.keys())):
            for input_key, lookup_key in self.connection_keys.items():
                connection_parameters[input_key]= input_dict[lookup_key]
            return connection_parameters
        else:
            logging.error('Could not find all required connection parameters')
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

    def _connect_with_sso(self, environment_config):
        try:
            if os.path.exists(self.token_cache_file):
                with open(self.token_cache_file, "r") as cache_file:
                    token_data = json.load(cache_file)
                    if time.time() < token_data.get("expiry", 0):
                        logging.debug("Using cached SSO token.")
                        session_params = token_data["session_params"]
                        return Session.builder.configs(session_params).create()

            session_params = {
                "account": environment_config["account"],
                "user": environment_config["user"],
                "authenticator": "externalbrowser",
                "database": environment_config.get("database"),
                "schema": environment_config.get("schema"),
                "warehouse": environment_config.get("warehouse"),  
                "role": environment_config.get("role"),
            }
        
            session = Session.builder.configs(session_params).create()

            token_data = {
                "session_params": session_params,
                "expiry": time.time() + 3600  
            }

            os.makedirs(os.path.dirname(self.token_cache_file), exist_ok=True)
            with open(self.token_cache_file, "w") as cache_file:
                json.dump(token_data, cache_file)

            return session

        except (ProgrammingError, DatabaseError) as db_error:
            logging.error(f"Snowflake connection error: {db_error}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error occurred during SSO authentication: {e}")
            raise

    def _get_session(self) -> Session:
        try:
            environment_config = self.config.get(self.environment)
            if not environment_config:
                raise ValueError(f"Environment '{self.environment}' not found in the TOML file.")

            e_config = self.config.get(self.environment, {})
            log_config = {key: e_config.get(key) for key in ['user', 'database', 'warehouse', 'role']}
            logging.info(f"Loaded configuration for {self.environment}: {log_config}")

            if "private_key_path" in environment_config:
                return self._connect_with_rsa(environment_config)
            elif "authenticator" in environment_config and environment_config["authenticator"] == "externalbrowser":
                return self._connect_with_sso(environment_config)
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
            logging.error(f"Error during query execution: {e}. Query: {query}. Skipping query execution.")
    
    def run_queries(self, queries: list, object_type: str = "") -> list:
        """
        Executes a list of queries in order and returns a list of output results.
        Logs and skips execution if no queries are provided.
        """
        outp = []
        if not queries:
            logging.debug(f"Skipping {object_type}, no queries to execute.")
            return outp

        for index, query in enumerate(queries):
            try:
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
            outp.append(self.session.file.put(file_config['local_path'], file_config['stage_path'], auto_compress=False, overwrite=True))
        return outp