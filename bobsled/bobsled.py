import logging
import sys
import argparse
from . import commands

class ArgHandler:
    def __init__(self, environment: str) -> None:
        self.environment = environment
        self.mapper = self._get_mapper()
        self.parser= self._get_parser()
    
    def _get_mapper(self) -> dict:
        logging.info(f"Initializing commands with environment: {self.environment}")
        mapper = {
            'deploy': commands.Deploy(self.environment),
            'init': commands.Init(self.environment),
            'clone': commands.Clone(self.environment),
            'run_script': commands.RunScript(self.environment),
            'test_dag': commands.TestDAG(self.environment),
        }
        return mapper

    def _get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description='The Bobsled command to execute')
        subparsers = parser.add_subparsers(help='parameters for the command', dest='cmd')
        for command in self.list_mapper_commands():
            logging.debug(command)
            self.add_command_parser(subparsers, command)
        return parser

    def add_command_parser(self, subparsers: argparse._SubParsersAction, command: commands.BobsledCommand) -> None:
        logging.debug(command.name)
        parser = subparsers.add_parser(command.name, help=command.help)
        for arg in command.args:
            logging.debug(arg.option)
            parser.add_argument(arg.option, required=arg.required, help=arg.help)

    def list_mapper_commands(self) -> list[commands.BobsledCommand]:
        return list(self.mapper.values())

    def get_command(self, parser) -> str:
        parsed_args = parser.parse_args()
        return parsed_args.command

    def exec(self):
        try:
            parsed_args = vars(self.parser.parse_args())
            logging.info(f"Parsed arguments: {parsed_args}")
            cmd = parsed_args.get('cmd')
            if not cmd:
                self.parser.print_help()
                sys.exit(1)
            self.mapper[cmd].run(parsed_args)
        except ValueError as ve:
            logging.error(f"Command not found: {ve}")
            sys.exit(1)
        except RuntimeError as rte:
            logging.error(f"Runtime error: {rte}")
            sys.exit(1)  
        except Exception as e:
            logging.error(f"Unexpected error during execution: {e}")
            sys.exit(1)

def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')

    # Parse the global arguments like environment
    global_parser = argparse.ArgumentParser(
        description="Bobsled Deployment Tool\n"
                    "Required: Please use -e to specify the environment.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    global_parser.add_argument("-e", "--environment", required=True, help="Specify the environment.")
    args, remaining_argv = global_parser.parse_known_args()

    # Initialize ArgHandler with the environment and parse the command-specific arguments
    handler = ArgHandler(environment=args.environment)

    # Reconfigure sys.argv to only include the remaining arguments after global parsing
    sys.argv = [sys.argv[0]] + remaining_argv

    handler.exec()

if __name__ == "__main__":
    main()