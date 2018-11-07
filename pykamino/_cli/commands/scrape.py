from pykamino._cli.commands import command

class Scrape(command.Command):
    def __init__(self, name=None, description=None):
        super().__init__(name, description)

    def __call__(self, args):
        print('Wow this is a daemon! Well, not quite.')

    def set_arguments(self, parser):
        pass
