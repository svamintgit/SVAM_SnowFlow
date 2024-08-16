import logging
import sys
import runner
import scripts

class Argument:
    def __init__(self, option, required, help):
        self.option=option
        self.required=required
        self.help = help

class BobsledCommand:
    def __init__(self) -> None:
        self.name = None
        self.help = None
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        pass

    def run(self, options) -> None:
        pass

class Deploy:
    def __init__(self) -> None:
        self.name = 'deploy'
        self.help = ' Specify no options to deploy account objects, just a db to deploy database objects, or a db and schema to deploy a schema. The account and database actually used are specified in the connection parameters  '
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        args = []
        args.append(Argument('-d', False, 'database name- should match the folder'))
        args.append(Argument('-s', False, 'schema name- should match the folder'))
        return args
    
    def run(self, args: dict) -> None:
        user = runner.SnowflakeUser()
        if args.get('d')==None:
            logging.info('Bobsled deploy account')
            self.account(user)
        elif args.get('s')==None:
            logging.info('Bobsled deploy db')
            self.database(user,args.d)
        else:
            logging.info('Bobsled deploy schema')
            self.schema(user,args.get('d'),args.get('s'))

    def account(self, user: runner.SnowflakeUser) -> None:
        user.session.query_tag= 'Bobsled deploy account'
        acct = scripts.SnowflakeAcct()
        user.run_queries(acct.get_roles())
        user.run_queries(acct.get_warehouses())
        user.run_queries(acct.get_integrations())
        logging.info('Account deployed')

    def database(self, user: runner.SnowflakeUser, db_name:str) -> None:
        logging.info('Bobsled deploy db')
        user.session.query_tag= 'Bobsled deploy db- '+db_name
        acct = scripts.SnowflakeAcct()
        db = scripts.SnowflakeDB(db_name,acct)
        user.run_queries(db.get_db_init())
        logging.info('DB Deployed')

    def schema(self, user: runner.SnowflakeUser, db_name:str, schema_name: str) -> None:
        logging.info('Bobsled deploy schema')
        user.session.query_tag= 'Bobsled deploy schema- '+db_name+'.'+schema_name
        acct = scripts.SnowflakeAcct()
        db = scripts.SnowflakeDB(db_name,acct)
        schema = scripts.SnowflakeSchema(schema_name, db)
        user.run_queries(schema.get_schema_init())
        user.session.use_schema(schema_name)
        user.run_queries(schema.get_stages())
        user.post_files(schema.get_staged_files())
        user.run_queries(schema.get_udfs())
        user.run_queries(schema.get_file_formats())
        user.run_queries(schema.get_tables())
        user.run_queries(schema.get_streams())
        user.run_queries(schema.get_views())
        user.run_queries(schema.get_stored_procs())
        user.run_queries(schema.get_tasks())
        user.run_queries(schema.get_dags())
        user.run_queries(schema.get_post_deploy())
        user.run_queries(schema.get_grants())
        logging.info('Schema deployed')

class Init:
    def __init__(self) -> None:
        self.name = 'init'
        self.help = 'initialize folder for account, database, or schema'
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        args = []
        args.append(Argument('-d', False, 'database name'))
        args.append(Argument('-s', False, 'schema name'))
        return args
    
    def run(self, args: dict) -> None:
        if args.get('d')==None:
            logging.info('Bobsled initialize account')
            self.account()
        elif args.get('s')==None:
            logging.info('Bobsled initialize db')
            self.database(args.get('d'))
        else:
            logging.info('Bobsled initialize schema')
            self.schema(args.get('d'),args.get('s'))

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
    def __init__(self) -> None:
        self.name = 'clone'
        self.help = 'clone a database or schema. specify just the database to clone the database, specify schema to clone a schema'
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        args = []
        args.append(Argument('-sd', True, 'source database name'))
        args.append(Argument('-ss', False, 'source schema name. specify to clone a schema'))
        args.append(Argument('-td', True, 'target database name'))
        args.append(Argument('-ts', False, 'target schema name. specify to clone a schema'))
        return args
    
    def run(self, args: dict) -> None:
        ss= args.get('ss')
        ts= args.get('ts')
        sd= args.get('sd')
        td= args.get('td')
        
        if ss==None and ts==None:
            #clone db
            query = 'create or replace database '+td+ ' clone '+sd
        elif bool(ss)!=bool(ts):
            #if we got one schema instead of two
            logging.error('one schema found. please submit both source and target schema if cloning schemas')
            query='' 
        else:
            #clone schema
            source = sd+'.'+ss
            tgt = td+'.'+ts
            query = 'create or replace schema '+tgt+' clone '+source
        user = runner.SnowflakeUser()
        logging.info(user.run_query(query))

class RunScript:
    def __init__(self) -> None:
        self.name = 'run_script'
        self.help = 'run a specific SQL script'
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        args = []
        args.append(Argument('-d', False, 'database name. specified in a use_database statement'))
        args.append(Argument('-s', False, 'schema name. specified in a use_schema statement'))
        args.append(Argument('-f', True, 'script path relative to the git root folder'))        
        return args
    
    def run(self, args: dict) -> None:
        database= args.get('d')
        schema= args.get('s')
        script_path= args.get('f')

        user = runner.SnowflakeUser()
        env = scripts.Environment()
        path = env.dh.get_absolute_path(script_path)
        queries = env.sp.read_file_queries(path)
        if database:
            user.session.use_database(database)
        if schema:
            user.session.use_schema(schema)
        logging.info(user.run_queries(queries))

class TestDAG:
    def __init__(self) -> None:
        self.name = 'test_dag'
        self.help = 'Test run a DAG file'
        self.args = self.get_args()

    def get_args(self) -> list[Argument]:
        args = []
        args.append(Argument('-d', True, 'database name'))
        args.append(Argument('-s', True, 'schema name'))
        args.append(Argument('-f', True, 'dag file name relative to dags folder'))
        return args
    
    def run(self, args: dict) -> None:
        database= args.get('d')
        schema_name= args.get('s')
        script_path= args.get('f')
        acct = scripts.SnowflakeAcct()
        db = scripts.SnowflakeDB(database,acct)
        schema = scripts.SnowflakeSchema(schema_name, db)
        queries= schema.get_dag(script_path)
        try:
            user = runner.SnowflakeUser()
            user.session.use_schema(schema_name)
            logging.info(user.run_queries(queries))
        except Exception as e:
            logging.error(e)
            logging.info('database '+database)
            logging.info('schema '+schema_name)

if __name__=="__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')
    config = {'d': 'mocj_db', 's': 'utility'}
    cmd = Init()
    logging.info(cmd.run(config))