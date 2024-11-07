import logging
import sys
import argparse
from . import commands

class ArgHandler:
    def __init__(self) -> None:
        self.mapper = self._get_mapper()
        self.parser = self._get_parser()
    
    def _get_mapper(self) -> dict:
        # Map Command Names to their classes
        mapper = {
            'deploy': commands.Deploy,
            'init': commands.Init,
            'clone': commands.Clone,
            'run_script': commands.RunScript,
            'test_dag': commands.TestDAG,
        }
        return mapper

    def _get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
        description=(
                'SnowFlow Deployment Tool\n'
                'SnowFlow is a command-line tool that simplifies and automates deployments, task management, and script execution within Snowflake environments.\n\n'
                'Usage:\n'
                '  - For general help, use "snowflow -h"\n'
                '  - For command-specific help, use "snowflow <command> -h"\n\n'
            ),
        usage=argparse.SUPPRESS,
        add_help=True,
        formatter_class=argparse.RawTextHelpFormatter
    )
        
        subparsers = parser.add_subparsers(title='Available Commands', metavar='', dest='cmd')
        
        for command_name, command_class in self.mapper.items():
            self.add_command_parser(subparsers, command_name, command_class)
        
        return parser

    def add_command_parser(self, subparsers: argparse._SubParsersAction, command_name: str, command_class: type) -> None:
        command_help = command_class.help
        command_args = command_class.get_args()
    
        parser = subparsers.add_parser(
            command_name,
            help=command_help,
            description=command_help,
            usage=argparse.SUPPRESS  
        )
    
        if command_name != 'init':
            parser.add_argument('-e', dest='environment', required=True, help='Specify the environment', metavar='')

        for arg in command_args:
            parser.add_argument(arg.option, required=arg.required, help=arg.help, metavar='')

    def exec(self):
        parsed_args = vars(self.parser.parse_args())
        cmd = parsed_args.pop('cmd', None)
        
        if not cmd:
            self.parser.print_help()
            sys.exit(1)
    
        command_class = self.mapper[cmd]
        if cmd == 'init':
            command_instance = command_class()
        else:
            environment = parsed_args.pop('environment', None)
            if not environment:
                raise ValueError("The '-e' argument is required for this command")
        
            command_instance = command_class(environment)

        command_instance.run(parsed_args)

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')

    handler = ArgHandler()
    handler.exec()

if __name__ == "__main__":
    main()