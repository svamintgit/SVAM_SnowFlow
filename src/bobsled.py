import logging
import sys
import argparse
import commands

class ArgHandler:
    def __init__(self) -> None:
        self.mapper = self._get_mapper()
        self.parser= self._get_parser()

    def _get_mapper(self) -> dict[commands.BobsledCommand]:
        #Specify the commands to include in this mapper 
        mapper = dict()
        mapper['deploy'] = commands.Deploy()
        mapper['init'] = commands.Init()
        mapper['clone'] = commands.Clone()
        mapper['run_script'] = commands.RunScript()
        mapper['test_dag'] = commands.TestDAG()
        return mapper
    
    def _get_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description='The Bobsled command to execute')
        subparsers = parser.add_subparsers(help='parameters for the command', dest='cmd')
        for command in self.list_mapper_commands():
            logging.debug(command)
            self.add_command_parser(subparsers, command)
        return parser

    def add_command_parser(self, subparsers:argparse._SubParsersAction, command: commands.BobsledCommand) -> None:
        logging.debug(command.name)
        parser = subparsers.add_parser(command.name, help=command.help)
        for arg in command.args:
            logging.debug(arg.option)
            parser.add_argument(arg.option, required = arg.required, help= arg.help)

    def list_mapper_commands(self) -> list[commands.BobsledCommand]:
        return list(self.mapper.values())

    def get_command(self, parser) -> str:
        parsed_args = parser.parse_args()
        return parsed_args.command
    
    def exec(self):
        parsed_args = vars(self.parser.parse_args())
        cmd = parsed_args.get('cmd')
        self.mapper[cmd].run(parsed_args)

if __name__=="__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(message)s')
    handler = ArgHandler()
    handler.exec()