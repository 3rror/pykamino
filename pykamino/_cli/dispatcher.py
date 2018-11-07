import argparse


class Dispatcher:
    def __init__(self, description=None):
        self.parser = argparse.ArgumentParser(description=description)
        self.subparsers = self.parser.add_subparsers()
        self.commands = []

    def add_command(self, command):
        self.commands.append(command)
        command.add_to_parser(self.subparsers)

    def run(self):
        args = self.parser.parse_args()
        try:
            args.action(args)
        except AttributeError:
            self.parser.print_help()