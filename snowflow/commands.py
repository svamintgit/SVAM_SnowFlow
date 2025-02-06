import logging
from . import runner
from . import scripts
from snowflake.connector.errors import ProgrammingError, DatabaseError
import sys

class Argument:
    def __init__(self, option, required, help):
        self.option = option
        self.required = required
        self.help = help

class SnowFlowCommand:
    def __init__(self) -> None:
        self.name = None
        self.help = None
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        pass

    def run(self, options) -> None:
        pass

class Deploy:
    help = 'Deploy account, database, or schema objects. Requires -e to specify the environment.'
    args = [
        Argument('-d', False, 'Specify Database name - Should match the folder'),
        Argument('-s', False, 'Specify Schema name - Should match the folder')
    ]

    def __init__(self, environment: str = None) -> None:
        self.name = 'Deploy'
        self.environment = environment
        self.args = self.get_args()
    
    @classmethod
    def get_args(cls):
        return cls.args
    
    def run(self, args: dict) -> None:
        if self.environment is None:
            raise ValueError("The '-e' argument is required for 'deploy' command.")
        try:
            user = runner.SnowflakeUser(self.environment)
            if args.get('d')==None:
                logging.info('Snowflow deploy account')
                self.account(user)
            elif args.get('s')==None:
                logging.info('Snowflow deploy db')
                self.database(user, args.get('d'))
            else:
                logging.info('Snowflow deploy schema')
                self.schema(user, args.get('d'), args.get('s'))
        except ValueError as ve:
            logging.error(f"Deployment error: {ve}")
            raise 
        except DatabaseError as de:
            logging.error(f"Database error during deployment: {de}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during deployment: {e}")
            logging.warning("Continuing execution; non-critical error")

    def account(self, user: runner.SnowflakeUser) -> None:
        user.session.query_tag= 'Snowflow deploy account'
        acct = scripts.SnowflakeAcct(self.environment)
        user.run_queries(acct.get_init(), object_type = "init")

        user.run_queries(acct.get_path_objects('roles'), object_type = "roles")
        user.run_queries(acct.get_path_objects('warehouses'), object_type = "warehouses")
        user.run_queries(acct.get_path_objects('integrations'), object_type = "integrations")
        user.run_queries(acct.get_path_objects('network_rules'), object_type = "network_rules")
        user.run_queries(acct.get_path_objects('network_policies'), object_type = "network_policies")
        user.run_queries(acct.get_grants(), object_type = "grants")
        logging.info('Account deployed')

    def database(self, user: runner.SnowflakeUser, db_name:str) -> None:
        user.session.query_tag= 'Snowflow Deploy Database: '+db_name
        acct = scripts.SnowflakeAcct(self.environment)
        db = scripts.SnowflakeDB(db_name,acct)
        user.run_queries(db.get_db_init())
        logging.info('Database Deployed')

    def schema(self, user: runner.SnowflakeUser, db_name:str, schema_name: str) -> None:
        user.session.query_tag= 'Snowflow Deploy Schema: '+db_name+'.'+schema_name
        acct = scripts.SnowflakeAcct(self.environment)
        db = scripts.SnowflakeDB(db_name,acct)
        schema = scripts.SnowflakeSchema(schema_name, db)

        user.run_queries(schema.get_schema_init(),  object_type = "init")
        user.session.use_schema(schema_name)

        user.run_queries(schema.get_path_objects('file_formats'), object_type = "file_formats")
        user.run_queries(schema.get_path_objects('stages'), object_type = "stages")
        user.post_files(schema.get_staged_files())
        user.run_queries(schema.get_path_objects('udfs'), object_type = "udfs")
        user.run_queries(schema.get_path_objects('tables'), object_type = "tables")
        user.run_queries(schema.get_path_objects('views'), object_type = "views")
        user.run_queries(schema.get_path_objects('streams'), object_type = "streams")
        user.run_queries(schema.get_path_objects('stored_procs', single_transaction=True), object_type = "stored_procs")
        user.run_queries(schema.get_path_objects('tasks'), object_type = "tasks")
        user.run_queries(schema.get_dags(), object_type = "dags")
        user.run_queries(schema.get_path_objects('post_deploy'), object_type = "post_deploy")
        user.run_queries(schema.get_grants(), object_type = "grants")
        logging.info('Schema Deployed')

class Init:
    help = 'Initialize folder structure for account, database, or schema. Does not require -e.'
    args = [
        Argument('-d', False, 'Specify Database name for initialization (optional)'),
        Argument('-s', False, 'Specify Schema name for initialization (optional)')
    ]

    def __init__(self) -> None:
        self.name = 'init'

    @classmethod
    def get_args(cls):
        return cls.args
    
    def run(self, args: dict) -> None:
        try:
            db_name = args.get('d')
            schema_name = args.get('s')
    
            if not db_name:
                logging.info('Snowflow initialize account')
                self.account()
            elif not schema_name:
                logging.info('Snowflow initialize database')
                self.database(db_name)
            else:
                logging.info('Snowflow initialize schema')
                self.schema(db_name, schema_name)
        except KeyError as ve:
            logging.error(f"Key error: {ve}. Check if database or schema name is provided correctly.")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during initialization: {e}")
            raise

    def account(self) -> None:
        acct = scripts.SnowflakeAcct()
        logging.info(acct.initialize())

    def database(self, db_name:str) -> None:
        acct = scripts.SnowflakeAcct()
        db = scripts.SnowflakeDB(db_name,acct)
        logging.info(db.initialize())

    def schema(self, db_name:str, schema_name: str) -> None:
        acct = scripts.SnowflakeAcct()
        db = scripts.SnowflakeDB(db_name,acct)
        schema = scripts.SnowflakeSchema(schema_name, db)
        logging.info(schema.initialize())

class Clone:
    help = 'Clone a Database or Schema. Specify just the database to clone the database. Specify schema to clone a schema. Requires -e to specify the environment.'
    args = [
        Argument('-sd', True, 'Source Database name'),
        Argument('-ss', False, 'Source Schema name. Specify to clone a schema'),
        Argument('-td', True, 'Target Database name'),
        Argument('-ts', False, 'Target Schema name. Specify to clone a schema')
    ]

    def __init__(self, environment: str = None) -> None:
        self.name = 'init'
        self.environment = environment

    @classmethod
    def get_args(cls):
        return cls.args
    
    def run(self, args: dict) -> None:
        try:
            ss= args.get('ss')
            ts= args.get('ts')
            sd= args.get('sd')
            td= args.get('td')
        
            if ss==None and ts==None:
                # Clone db
                query = 'create or replace database '+td+ ' clone '+sd
            elif bool(ss)!=bool(ts):
                # If we got one schema instead of two
                logging.error('One schema found. Please submit both source and target schema if cloning schemas')
                query='' 
            else:
                #clone schema
                source = sd+'.'+ss
                tgt = td+'.'+ts
                query = 'create or replace schema '+tgt+' clone '+source
            user = runner.SnowflakeUser(self.environment)
            logging.info(user.run_query(query))

        except ValueError as ve:
            logging.error(f"Clone error: {ve}")
            raise
        except DatabaseError as de:
            logging.error(f"Database error during cloning: {de}")
            raise
        except ProgrammingError as pe:
            logging.error(f"Programming error during cloning: {pe}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during cloning: {e}")
            raise

class RunScript:
    help = 'Run a specific SQL script. Requires -e to specify the environment.'
    args = [
        Argument('-d', False, 'Database name. Specified in a use_database statement'),
        Argument('-s', False, 'Schema name. Specified in a use_schema statement'),
        Argument('-f', True, 'Script Path relative to the Root folder')
    ]

    def __init__(self, environment: str = None) -> None:
        self.name = 'init'
        self.environment = environment

    @classmethod
    def get_args(cls):
        return cls.args
    
    def run(self, args: dict) -> None:
        try:
            database= args.get('d')
            schema= args.get('s')
            script_path= args.get('f')

            user = runner.SnowflakeUser(self.environment)
            env = scripts.Environment(self.environment)
            path = env.dh.get_absolute_path(script_path)
            queries = env.sp.read_file_queries(path)
            if database:
                user.session.use_database(database)
            if schema:
                user.session.use_schema(schema)
            logging.info(user.run_queries(queries))

        except ValueError as ve:
            logging.error(f"RunScript error: {ve}")
            raise
        except DatabaseError as de:
            logging.error(f"Database error while running script: {de}")
            raise
        except Exception as e:
            logging.warning(f"Error during script execution: {e}. Skipping the script.")

class TestDAG:
    help = 'Test run a DAG file. Requires -e to specify the environment.'
    args = [
        Argument('-d', True, 'Database name'),
        Argument('-s', True, 'Schema name'),
        Argument('-f', True, 'DAG file name relative to DAG folder')
    ]

    def __init__(self, environment: str = None) -> None:
        self.name = 'init'
        self.environment = environment

    @classmethod
    def get_args(cls):
        return cls.args
    
    def run(self, args: dict) -> None:
        database= args.get('d')
        schema_name= args.get('s')
        script_path= args.get('f')
        acct = scripts.SnowflakeAcct(self.environment)
        db = scripts.SnowflakeDB(database,acct)
        schema = scripts.SnowflakeSchema(schema_name, db)
        queries= schema.get_dag(script_path)
        try:
            user = runner.SnowflakeUser(self.environment)
            user.session.use_schema(schema_name)
            logging.info(user.run_queries(queries))
        except DatabaseError as de:
            logging.error(f"Database error during DAG test: {de}")
            raise
        except ProgrammingError as pe:
            logging.error(f"Programming error during DAG test: {pe}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error during DAG test: {e}")
            raise

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')
