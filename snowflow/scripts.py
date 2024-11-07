from pathlib import Path
import yaml
import logging
import networkx as nx
import sys
import os
import re

class ScriptParser:
    def __init__(self, substitutions=dict()):
        self.substitutions: dict = substitutions or {}

    def parse_yaml_file(self, file_path: Path) -> dict:
        '''
        Load a yaml file as a python dictionary
        '''
        try:
            with open(file_path, 'r') as raw_yaml:
                logging.info(f"Opened YAML file at {file_path}")
                string_file = raw_yaml.read()
                string_file = string_file.replace('\t','  ')
                logging.info(f"File content before substitution: {string_file}")
                string_file = self.substitute_vars(string_file)
                data = yaml.safe_load(string_file) or {}
                logging.info(f"Parsed YAML data: {data}")
                return data
        except FileNotFoundError as e:
            logging.error(f"YAML file not found at path: {file_path}. Error: {e}")
            return {}
        except yaml.YAMLError as e:
            logging.error(f"YAML parsing error in {file_path}. Error: {e}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while parsing YAML file: {e}")
            raise

    def get_path_yamls(self, path: Path) -> list[dict]:
        try:
            yamls=[]
            for f in path.glob("*.yaml"):
                yamls.append(self.parse_yaml_file(f))
            return yamls
        except FileNotFoundError as e:
            logging.error(f"YAML directory not found: {e}")
            raise
        except Exception as e:
            logging.warning(f"Error while retrieving YAML files: {e}. Skipping.")
            return []

    def clean_query(self, query: str) -> str:
        '''
        Given a SQL query, substitute variables and clean up invalid characters
        '''
        query = self.strip_special_chars(query)
        query = self.substitute_vars(query)
        return query

    def substitute_vars(self, query: str) -> str:
        try:
            if self.substitutions:
                for var, val in self.substitutions.items():
                    query = query.replace(var, str(val))
            return query
        except TypeError as te:
            logging.error(f"TypeError in substitute_vars: {te}. Substitutions: {self.substitutions}")
            raise
        except Exception as e:
            logging.error(f"Unexpected error in substitue_vars: {e}")
            logging.error(f"Query: {query}, Substitutions: {self.substitutions}")
            raise
    
    def strip_special_chars(self, query: str) -> str:
        try:
            return query.rstrip()
        except Exception as e:
            logging.error(f"Unexpected error in strip_special_chars: {e}")
            raise

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
        """
        Reads a file, checking the current working directory first.
        """
        # Check in the current working directory first
        user_path = os.path.join(os.getcwd(), path)
    
        if os.path.exists(user_path):
            try:
                with open(user_path, 'r') as file:
                    content = file.read()
                return content
            except Exception as e:
                logging.error(f"Error reading file {user_path}: {e}")
                return ''
        else:
            logging.error(f"File not found: {user_path}. Ensure the file exists.")
            return ''

    
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
        """
        Loop over path, return contents of files in a list of queries.
        Looks in the current working directory.
        """
        user_path = os.path.join(os.getcwd(), path)
    
        if not os.path.exists(user_path):
            logging.error(f"Directory not found: {user_path}")
            return []

        queries = []
        for f in Path(user_path).glob("*.sql"):
            if single_transaction:
                queries.append(self.read_clean_file(f))
            else:
                queries.extend(self.read_file_queries(f))
    
        logging.debug(queries)
        return queries

        
class DirectoryHandler:
    def __init__(self):
        self.root_dir = os.getcwd()
        self.sql_templates_path = Path(self.root_dir, 'snowflow','sql_templates')

    def mkdir(self, path: Path) -> Path:
        try:
            logging.info('Making the directory if not exists: ' + str(path))
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
        
        project_root = Path(self.root_dir) 
        query_vars_file = project_root / 'query_variables.yaml'

        if not query_vars_file.exists():
            try:
                query_vars_file.touch()
                logging.info("Making query_variables.yaml if not exists: " + str(project_root))
            except Exception as e:
                logging.error(f"Failed to create query_variables.yaml: {e}")

        return True

    def get_absolute_path(self, relative_path: str) -> Path:
        """
        Given a path relative to the root as a string, return a Path object based on the current working directory.
        """
        return Path(os.path.join(os.getcwd(), 'snowflake', relative_path))

class Environment:
    def __init__(self, environment: str):
        if not environment:
            raise ValueError("Environment not specified. Please provide a valid environment.")
        self.dh = DirectoryHandler()
        self.sp = ScriptParser()
        self.query_variables_file = 'query_variables.yaml'
        self.env_var = environment  
        self.query_variables = self.get_query_variables(self.env_var) 
        self.sp.substitutions = self.query_variables or {}

    def get_query_variables(self, env):
        try:
            local_path = os.path.join(os.getcwd(), 'query_variables.yaml')
            logging.debug(f"Looking for query variables in: {local_path}")

            if not os.path.exists(local_path):
                logging.info("query_variables.yaml not found. Proceeding without substitutions.")
                return {}

            raw_vars = self.sp.parse_yaml_file(local_path)

            logging.info(f"Contents of raw_vars after parsing: {raw_vars}")
        
            if not raw_vars:
                logging.info("query_variables.yaml is empty.")
                return {}

            if env not in raw_vars:
                logging.info(f"No variables found for environment '{env}' in query_variables.yaml.")
                return {}

            logging.info(f"Query variables found for environment '{env}': {raw_vars[env]}")
            return raw_vars[env]
        
        except Exception as e:
            logging.error(f"Unexpected error in get_query_variables: {e}")
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
    def __init__(self, environment: str = None) -> None:
        if environment:
            self.environment = Environment(environment)
            self.sp = self.environment.sp
            self.dh = self.environment.dh
        else:
            self.environment = None
            self.sp = ScriptParser()
            self.dh = DirectoryHandler() 
        
        self.env_dir = Path(os.getcwd(), 'snowflake')
        self.child_objects = ['databases', 'integrations', 'roles', 'warehouses', 'network_rules', 'network_policies']
        self.child_lookup = self.dh.get_path_lookup(self.env_dir, self.child_objects)

    def initialize(self) -> None:
        Path(self.env_dir, 'init.sql').touch()
        Path(self.env_dir, 'grants.sql').touch()
    
    def get_roles(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['roles'])
    
    def get_warehouses(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['warehouses'])
    
    def get_integrations(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['integrations'])
    
    def get_network_rules(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['network_rules'])
    
    def get_network_policies(self) -> list[str]:
        return self.sp.get_path_queries(self.child_lookup['network_policies'])

    def get_grants(self) -> list[str]:
        return self.sp.read_file_queries(Path(self.env_dir, 'grants.sql'))
    
    def run_global_init(self) -> list[str]:
        global_init_path = Path(self.env_dir, 'init.sql')
        return self.sp.read_file_queries(global_init_path)
    
    def get_databases(self) -> list:
        #return all the database objects
        return [SnowflakeDB(d.stem,self) for d in self.child_lookup['databases'].iterdir() if d.is_dir()]

class SnowflakeDB:
    def __init__(self, name: str, account: SnowflakeAcct):
        self.account=account
        self.sp = account.sp
        self.name=name
        self.dh = account.dh

        self.path = Path(os.getcwd(), 'snowflake', 'databases', self.name)
        self.child_objects= ['schemas', 'dml']
        self.path_lookup = self.dh.get_path_lookup(self.path, self.child_objects)
        self.dml_path = Path(self.path, 'dml')
        self.grants_files = Path(self.path, 'grants.sql')
        self.init_file = Path(self.path, 'init.sql')
    
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
        self.name = name
        self.database = database
        self.account= self.database.account
        self.environment = self.account.environment

        if self.environment:
            self.query_variables = self.environment.query_variables
            self.sp = database.sp
            self.sp.substitutions = self.sp.substitutions | self.query_variables
        else:
            self.query_variables = {}
            self.sp = ScriptParser()
        
        self.dh = database.dh
        self.schema_path = Path(os.getcwd(), 'snowflake', 'databases', self.database.name, 'schemas', self.name)
        self.child_objects= ['file_formats', 'tables','streams','stages','views','tasks','dags','udfs', 'stored_procs', 'staged_files', 'post_deploy']
        self.path_lookup = self.dh.get_path_lookup(self.schema_path, self.child_objects)
    
    def __str__(self):
        return self.database.name+'.'+self.name
    
    def __repr__(self):
        return self.database.name+'.'+self.name
    
    def _get_queries_or_empty(self, object_type: str) -> list[str]:
        """
        Method to get queries for object type
        If the folder does not exist, returns an empty list
        """
        object_path = self.path_lookup.get(object_type)
        if object_path and object_path.exists():
            logging.debug(f"Fetching queries for {object_type}")
            queries = self.sp.get_path_queries(object_path)
            if queries:
                return queries
            else:
                return []
        else:
            logging.debug(f"Skipping {object_type}, folder does not exist.")
            return []
        
    
    def get_ordered_views(self) -> list[str]:
        """
        Returns a list of view queries in order, based on file name.
        """
        view_path = self.path_lookup.get('views')
        if not view_path or not view_path.exists():
            logging.debug("Skipping views, folder does not exist.")
            return []

        view_files = sorted(view_path.glob("*.sql"), key=lambda f: f.stem)

        queries = []
        for view_file in view_files:
            file_queries = self.sp.read_file_queries(view_file)
            if file_queries:
                queries.extend(file_queries)
            else:
                logging.debug(f"No queries found in file for view '{view_file.stem}'. Skipping.")
        
        return queries
    
    def get_schema_init(self) -> list[str]:
        return self.sp.read_file_queries(Path(self.schema_path,'init.sql'))

    def get_schema_grants(self) -> list[str]:
        return self._get_queries_or_empty('grants')

    def initialize(self):
        self.account.initialize()
        self.database.initialize()
        self.dh.initialize_directory(self.schema_path, self.path_lookup)
        return True
    
    def get_file_formats(self) -> list[str]:
        return self._get_queries_or_empty('file_formats')
    
    def get_tables(self) -> list[str]:
        return self._get_queries_or_empty('tables')

    def get_streams(self) -> list[str]:
        return self._get_queries_or_empty('streams')
    
    def get_stages(self) -> list[str]:
        return self._get_queries_or_empty('stages')
    
    def get_views(self) -> list[str]:
        return self.get_ordered_views()
    
    def get_tasks(self) -> list[str]:
        return self._get_queries_or_empty('tasks')
    
    def get_udfs(self) -> list[str]:
        return self._get_queries_or_empty('udfs')
    
    def get_stored_procs(self) -> list[str]:
        return self.sp.get_path_queries(self.path_lookup.get('stored_procs'), single_transaction=True) if self.path_lookup.get('stored_procs') else []
    
    def get_post_deploy(self) -> list[str]:
        return self._get_queries_or_empty('post_deploy')

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
        curr_dag_path = Path(self.path_lookup['dags'], file_name) if self.path_lookup.get('dags') else None
        if curr_dag_path:
            logging.debug(curr_dag_path)
            cd = self.sp.parse_yaml_file(curr_dag_path)
            dag = TaskDAG(cd, self)
            return dag.get_all_queries()
        else:
            logging.debug(f"Skipping dag {file_name}, folder does not exist.")
            return []
    
    def get_staged_files(self) -> list[dict]:
        """
        Return a list of dicts with the format {'local_path': ,'stage_path':}
        where stage is a stage name that corresponds to a subfolder name in the staged_files
        and path is the filename and path within the folder.
        """
        outp = []
        staged_files_path = Path(self.schema_path, 'staged_files')
        if staged_files_path.exists():
            stages = [p for p in list(staged_files_path.glob('*')) if p.is_dir()]
            for stage in stages:
                local_stage = Path(staged_files_path, stage)
                files = [f for f in list(stage.glob('**/*')) if f.is_file()]
                for file in files:
                    local_path = str(file)
                    stage_path = '@' + stage.name + '/' + file.relative_to(local_stage).parent.as_posix()
                    outp.append({'local_path': local_path, 'stage_path': stage_path})
        else:
            logging.debug(f"Skipping staged files, folder does not exist.")
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
        logging.debug(var_dict)
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
        logging.debug(template)
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
        sql_proc_code = self.sp.read_clean_file(proc_path)

        print(f"Generated SQL Procedure for {self.name}: \n{sql_proc_code}")
        return sql_proc_code
    
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