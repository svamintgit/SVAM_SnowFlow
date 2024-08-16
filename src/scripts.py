from pathlib import Path
import yaml
import logging
import networkx as nx
import sys
import os

class ScriptParser:
    def __init__(self, substitutions=dict()):
        self.substitutions: dict = substitutions

    def parse_yaml_file(self, file_path: Path) -> dict:
        '''
        Load a yaml file as a python dictionary
        '''
        try:
            with open(file_path, 'r') as raw_yaml:
                string_file = raw_yaml.read()
                string_file.replace('\t','  ')
                string_file = self.substitute_vars(string_file)
                dict = yaml.safe_load(string_file)
            return dict
        except Exception as e:
            logging.error(e)
            return {}

    def get_path_yamls(self, path: Path) -> list[dict]:
        try:
            yamls=[]
            for f in path.glob("*.yaml"):
                yamls.append(self.parse_yaml_file(f))
            return yamls
        except Exception as e:
            logging.error(e)
            return [{}]

    def clean_query(self, query: str) -> str:
        '''
        Given a SQL query, substitute variables and clean up invalid characters
        '''
        query = self.strip_special_chars(query)
        query = self.substitute_vars(query)
        return query

    def substitute_vars(self, query: str) -> str:
        try:
            for var, val in self.substitutions.items():
                query= query.replace(var, str(val))
        except Exception as e:
            logging.error(e)
            logging.error('query = '+query)
            logging.error(self.substitutions)
        finally:
            return query
    
    def strip_special_chars(self, query: str) -> str:
        try:
            return query.rstrip()
        except Exception as e:
            logging.error(e)
            return query

    def clean_query_list(self, queries: list) -> list[str]:
        try:
            cleaned_list = []
            for query in queries:
                cleaned = self.clean_query(query)
                if cleaned!='':
                    cleaned_list.append(cleaned)
            return cleaned_list
        except Exception as e:
            logging.error(e)
            return queries

    def read_clean_file(self, path: Path) -> str:
        return self.clean_query(self.read_file(path))
    
    def read_file(self, path: Path) -> str:
        try:
            content = open(path).read()
        except Exception as e:
            logging.error(e)
            return ''
        else:
            return content
    
    def read_file_queries(self, path: Path, single_transaction=False) -> list[str]:
        '''
        read a sql file that can contain multiple queries
        '''
        try:
            if single_transaction:
                clean = [self.read_clean_file(path)]
            else:
                clean = self.read_clean_file(path).strip(';').split(';')
            return clean
        except Exception as e:
            logging.error(e)
            return []
    

    def get_path_queries(self, path: Path, single_transaction: bool = False) -> list[str]:
        '''
        Loop over path, return contents of files in a list of queries
        '''
        try:
            queries=[]
            for f in path.glob("*.sql"):
                if single_transaction:
                    queries.append(self.read_clean_file(f))
                else:
                    queries.extend(self.read_file_queries(f))
        except Exception as e:
            logging.error(e)
            return []
        else:
            logging.debug(queries)
            return queries
        
class DirectoryHandler:
    def __init__(self):
        self.root_dir = Path(__file__).resolve().parents[1]
        self.sql_templates_path = Path(self.root_dir, 'src','sql_templates')

    def mkdir(self, path: Path) -> Path:
        try:
            logging.info('Making the dir if not exists '+str(path))
            path.mkdir(parents=False, exist_ok=True)
            return path
        except FileNotFoundError as e:
            logging.error(e)
            logging.error('Could not find parent directory')
            sys.exit(1)

    def get_path_lookup(self, root: Path, child_list: list) -> dict:
        #given a list of child objects, create a lookup dictionary
        return {c: Path(root, c) for c in child_list}

    def initialize_directory(self, root: Path, child_lookup: dict) -> dict:
        #create root and child directories
        self.mkdir(root)
        for name, path in child_lookup.items():
            self.mkdir(path)
        Path(root, 'init.sql').touch()
        Path(root, 'grants.sql').touch()
        return True
    
    def get_absolute_path(self, relative_path: str):
        #given a path relative to the root as a str, return a Path object
        return Path(self.root_dir, relative_path)

class Environment:
    def __init__(self):
        self.dh = DirectoryHandler()
        self.sp = ScriptParser()
        self.local_connection_file='local_connection.yaml'
        self.query_variables_file='query_variables.yaml'
        self.param_reqs = ['SNOWSQL_ACCOUNT','SNOWSQL_USER','SNOWSQL_PWD','SNOWSQL_WAREHOUSE','SNOWSQL_ROLE','SNOWSQL_DB', 'BOBSLED_ENV']
        self.env_var='BOBSLED_ENV'
        self.params = self.get_params()
        self.bobsled_env = self.params.get(self.env_var)
        self.query_variables = self.get_query_variables(self.bobsled_env)
        self.sp.substitutions = self.query_variables 
    
    def get_params(self):
        local= self._get_local_params()
        if len(local.keys()) == 0:
            logging.info('no local keys, using env')
            params = self._get_env_params()
        else:
            params = local
        valid = self._validate_params(params)
        if valid:
            return params
        else:
            logging.error('Input parameters are invalid. Please check that your local_connection file or environment variables contain '+str(self.param_reqs))
            sys.exit(1)
    
    def _get_local_params(self) -> dict:
        '''
        Read the connection parameters from the local_connection.yaml file
        '''
        try:
            local_path = self.dh.get_absolute_path(self.local_connection_file)
            raw_params = self.sp.parse_yaml_file(local_path)
            return raw_params
        except Exception as e:
            logging.error(e)
            return {}
        
    def _get_env_params(self) -> dict:
        '''
        Read the connection parameters from the environment variables
        '''
        return dict(os.environ)

    def _validate_params(self, params: dict)-> bool:
        if set(self.param_reqs).issubset(set(params.keys())):
            return True
        else:
            logging.error('could not find all required keys. missing the following:')
            logging.error(set(self.param_reqs).difference(set(params.keys()))) 
            return False

    def get_query_variables(self, env):
        '''
        Get a dict containing all query variables for the environment
        '''
        try:
            local_path = self.dh.get_absolute_path(self.query_variables_file)
            raw_vars = self.sp.parse_yaml_file(local_path)
            return raw_vars[env]
        except Exception as e:
            logging.error(e)
            return {}

    def initialize(self):
        '''
        Make the local connection and query variables file
        '''
        pass

class SnowflakeAcct:
    '''
    Provides an interface between the repo files and a Snowflake account 
    '''
    def __init__(self) -> None:
        self.environment = Environment()
        self.sp = self.environment.sp
        self.dh = self.environment.dh
        self.env_dir= Path(self.dh.root_dir, 'snowflake')
        self.child_objects= ['databases', 'integrations', 'roles','warehouses']
        self.child_lookup = self.dh.get_path_lookup(self.env_dir, self.child_objects)

    def initialize(self) -> None:
        logging.info(self.dh.initialize_directory(self.env_dir, self.child_lookup))
        logging.info(Path(self.env_dir,self.environment.query_variables_file).touch())
    
    def get_roles(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['roles'])
    
    def get_warehouses(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['warehouses'])
    
    def get_integrations(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['integrations'])

    def get_grants(self) -> list[str]:
        return self.sp.read_file_queries(Path(self.env_dir, 'grants.sql'))
    
    def get_databases(self) -> list:
        #return all the database objects
        return [SnowflakeDB(d.stem,self) for d in self.child_lookup['databases'].iterdir() if d.is_dir()]

class SnowflakeDB:
    def __init__(self, name: str, account: SnowflakeAcct):
        self.account=account
        self.sp = account.sp
        self.name=name
        self.dh = account.dh
        self.path = Path(self.account.env_dir, 'databases',self.name)
        self.child_objects= ['schemas', 'dml']
        self.path_lookup = self.dh.get_path_lookup(self.path, self.child_objects)
        self.dml_path = Path(self.path, 'dml')
        self.grants_files = Path(self.path, 'grants.sql')
        self.init_file = Path(self.path, 'grants.sql')
    
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return self.name
    
    def get_db_init(self):
        return self.sp.read_file_queries(self.init_file)
    
    def get_grants(self):
        return self.sp.get_path_queries(self.grants_files)
    
    def get_schemas(self):
        return [SnowflakeSchema(s.stem,self) for s in self.path_lookup['schemas'].iterdir() if s.is_dir()]
    
    def get_file_queries(self, file_path):
        '''
        For testing purposes. Given a file, extract the query
        '''
        return self.sp.read_file_queries(Path(self.path,file_path))

    def initialize(self):
        self.account.initialize()
        self.dh.initialize_directory(self.path, self.path_lookup)
        return True

class SnowflakeSchema:
    def __init__(self, name: str, database: SnowflakeDB):
        self.name=name
        self.database = database
        self.account= self.database.account
        self.environment = self.account.environment
        self.query_variables= self.environment.query_variables
        self.sp = database.sp
        self.dh = database.dh
        self.sp.substitutions = self.sp.substitutions | self.query_variables
        self.schema_path = Path(self.database.path,'schemas',self.name)

        self.child_objects= ['file_formats', 'tables','streams','stages','views','tasks','dags','udfs', 'stored_procs', 'staged_files', 'post_deploy']
        self.path_lookup = self.dh.get_path_lookup(self.schema_path, self.child_objects)
    
    def __str__(self):
        return self.database.name+'.'+self.name
    
    def __repr__(self):
        return self.database.name+'.'+self.name
    
    def get_schema_init(self) -> list[str]:
        return self.sp.read_file_queries(Path(self.schema_path,'init.sql'))

    def get_schema_grants(self) -> list[str]:
        return self.sp.get_path_queries(Path(self.schema_path,'grants'))

    def initialize(self):
        self.account.initialize()
        self.database.initialize()
        self.dh.initialize_directory(self.schema_path, self.path_lookup)
        return True
    
    def get_file_formats(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['file_formats'])
    
    def get_tables(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['tables'])

    def get_streams(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['streams'])
    
    def get_stages(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['stages'])
    
    def get_views(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['views'])
    
    def get_tasks(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['tasks'])
    
    def get_udfs(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['udfs'])
    
    def get_stored_procs(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['stored_procs'], single_transaction=True)
    
    def get_post_deploy(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup['post_deploy'])

    def get_grants(self) -> list[str]:
        return self.sp.read_file_queries(Path(self.schema_path,'grants.sql')) 
    
    
    def get_dags(self) -> list[str]:
        queries = []
        dag_list = [TaskDAG(cd, self) for cd in self.sp.get_path_yamls(self.path_lookup['dags'])]
        for dag in dag_list: 
            queries.extend(dag.get_all_queries())
        return queries

    def get_dag_objs(self):
        return [TaskDAG(cd, self) for cd in self.sp.get_path_yamls(self.path_lookup['dags'])]

    def get_dag(self, file_name) -> list[str]:
        curr_dag_path = Path(self.path_lookup['dags'],file_name)
        logging.debug(curr_dag_path)
        cd = self.sp.parse_yaml_file(curr_dag_path)
        dag = TaskDAG(cd, self)
        return dag.get_all_queries()

    def get_staged_files(self) -> list[dict]:
        '''
        Return a list of dicts with the format {'local_path': ,'stage_path':}
        where stage is a stage name that corresponds to a subfolder name in the staged_files
        and path is the filename and path within the folder
        '''
        outp=[]
        staged_files_path= Path(self.schema_path,'staged_files')
        stages = [p for p in list(staged_files_path.glob('*')) if p.is_dir()]
        for stage in stages:
            local_stage = Path(staged_files_path, stage)
            files = [f for f in list(stage.glob('**/*')) if f.is_file()]
            for file in files:
                local_path =  str(file)
                stage_path = '@'+stage.name+'/'+file.relative_to(local_stage).parent.as_posix()
                outp.append({'local_path':local_path,'stage_path':stage_path})
        return outp

class TaskDAG:
    def __init__(self, config_dict: dict, schema: SnowflakeSchema):
        self.config_dict=config_dict
        logging.info(self.config_dict)
        self.schema= schema
        self.name = self.config_dict.get('DAG_NAME')
        self.root= self.config_dict.get('ROOT_TASK')
        self.sp = schema.sp
        self.dh = schema.dh
        self.query_variables = self.get_query_variables()
        self.task_template = self._get_task_template()
        self.sp.substitutions = self.query_variables
        self.task_dict, self.digraph = self._get_structs()
        
    
    def _get_task_dict(self) -> dict:
        #check for duplicate names
        #check for required value
        task_dict = dict()
        for t_config in self.config_dict['TASKS']:
            task = Task(t_config)
            task_dict[task.name]=task
        return task_dict
    
    def _get_structs(self):
        digraph  = nx.DiGraph()
        task_dict = dict()
        for t_config in self.config_dict['TASKS']:
            curr_name = t_config['NAME']
            dependencies = t_config.get('DEPENDS_ON',[])
            digraph.add_node(curr_name)
            for dependency in dependencies:
                digraph.add_edge(dependency,curr_name)

            task = Task(t_config, self)
            task_dict[task.name]=task
        return task_dict, digraph

    def get_all_queries(self):
        query_list=[]
        create_order = list(nx.topological_sort(self.digraph))
        for task in create_order:
            t = self.task_dict[task]
            query_list.extend(t.get_create_queries())
        if self.config_dict.get('ENABLED')=='TRUE':
            dependents =  "SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('"+self.root+"')"
            root_enable = "ALTER TASK "+self.root+" RESUME"
            query_list.append(dependents)
            query_list.append(root_enable)
        else:
            logging.info('suspend task')
            root_disable = "ALTER TASK "+self.root+" SUSPEND"
            query_list.append(root_disable)
        return query_list
    
    def get_query_variables(self) -> dict:
        var_dict = self.schema.query_variables
        keys = ['ROOT_TASK','INITIAL_WAREHOUSE_SIZE','ALLOW_OVERLAPPING_EXECUTION','WAREHOUSE']
        for key in keys:
            var_dict['!!!'+key+'!!!'] = self.config_dict.get(key)
        logging.info(var_dict)
        return var_dict
    
    def _get_task_template(self) -> str:
        '''
        Returns the file name of the template to use
        '''
        template = 'NO_TEMPLATE_FOUND.sql'
        if 'INITIAL_WAREHOUSE_SIZE' in self.config_dict.keys():
            template = 'sf_task_template.sql'
        elif 'WAREHOUSE' in self.config_dict.keys():
            template = 'user_task_template.sql'
        elif 'INITIAL_WAREHOUSE_SIZE' in self.config_dict.keys() and 'WAREHOUSE' in self.config_dict.keys():
            logging.error('Could not determine template to use. Please specify a variable of either INITIAL_WAREHOUSE_SIZE or WAREHOUSE')
        else:
            logging.error('Could not determine template to use. Please specify a variable of either INITIAL_WAREHOUSE_SIZE or WAREHOUSE')
        logging.info(template)
        return template

class Task:
    def __init__(self, config_dict: dict, dag: TaskDAG):
        self.config_dict = config_dict
        self.name = self.config_dict.get('NAME')
        self.dag= dag
        self.schema = dag.schema
        self.database= self.schema.database
        self.account= self.database.account
        self.is_root = self._is_root()
        self.script_path = Path(self.database.dml_path, self.config_dict.get('SCRIPT_PATH'))
        self.sp = ScriptParser(self.dag.sp.substitutions.copy())
        self.sp.substitutions.update(self.get_query_variables())
        self.templates_folder = dag.dh.sql_templates_path
    
    def get_create_queries(self) -> list[str]:
        l = list()
        if self.is_root:
            l.append('ALTER TASK IF EXISTS '+self.name+ ' suspend')
        l.append(self.get_sql_proc_code())
        l.append(self.get_task_code())
        return l

    def _is_root(self) -> bool:
        if self.name == self.dag.root:
            return True
        else:
            return False
    
    def get_sql_proc_code(self) -> list[str]: 
        proc_path = Path(self.templates_folder, 'sql_procedure_template.sql')
        return self.sp.read_clean_file(proc_path)
    
    def get_when_clause(self) -> str:
        value = self.config_dict.get('WHEN', None)
        if not value:
            return ' '
        else:
            return 'WHEN '+value+' '
    
    def get_query_variables(self) -> dict:
        var_dict=dict()
        var_dict['!!!DAG_NAME!!!']=self.dag.name
        var_dict['!!!TASK_NAME!!!']=self.name
        var_dict['!!!PROC_NAME!!!']=self.name+'_sp'
        var_dict['!!!WHEN_CLAUSE!!!'] = self.get_when_clause()
        var_dict['!!!SCRIPT_CODE!!!'] = self.get_script_code()
        if self.is_root:
            overlap = 'ALLOW_OVERLAPPING_EXECUTION = '+self.dag.config_dict.get('ALLOW_OVERLAPPING_EXECUTION') 
            suspend = 'SUSPEND_TASK_AFTER_NUM_FAILURES = 1'
            var_dict['!!!ROOT_VARIABLES!!!']=overlap+' '+suspend
            var_dict['!!!SCHEDULE!!!']= "SCHEDULE = '"+self.dag.config_dict.get('SCHEDULE','')+"'"
        else:
            var_dict['!!!ROOT_VARIABLES!!!']= ''
            var_dict['!!!SCHEDULE!!!'] = 'AFTER '+','.join(self.config_dict.get('DEPENDS_ON',''))
        return var_dict

    def get_script_code(self) -> str:
        query_list =  self.sp.read_file_queries(self.script_path)
        return ';'.join(query_list)
    
    def get_schedule(self) -> str:
        #if root, "SCHEDULE = 'USING CRON 0 8 * * * America/New_York'"
        if self.is_root:
            schedule = "SCHEDULE = '"+self.dag.config_dict.get('SCHEDULE','')+"'"
        else:
            schedule = 'AFTER '+','.join(self.config_dict.get('DEPENDS_ON',''))
        return schedule
    
    def get_task_code(self) -> str:
        template_path = Path(self.templates_folder, self.dag.task_template)
        return self.sp.read_clean_file(template_path)

if __name__=="__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')
    #sp=ScriptParser()
    #logging.info(sp.substitutions)
    e =  Environment()
    logging.info(e.query_variables)
    acct= SnowflakeAcct()
    db= SnowflakeDB('mocj_db', acct)
    schema= SnowflakeSchema('cja',db)
    dags = schema.get_dag_objs()
    for dag in dags:
        name = dag.name
        if name=='cja_live':
            logging.info(dag.task_dict)
            logging.info(dag.digraph.edges(data=True))
            for task in dag.task_dict.values():
                logging.info(task.name)
                logging.info(task.sp.substitutions)
                #logging.info(task.get_create_queries())
            '''
            qs = dag.get_all_queries()
            for q in qs:
                logging.info(q[:50])
            #logging.info(dag.get_all_queries())
            '''