from pykamino._cli.commands import command
from pykamino.scraper.scraper import Scraper

class Scrape(command.Command):
    def __init__(self, name=None, description=None):
        super().__init__(name, description)

    def __call__(self, args):
        if 'start' in args:
            scraper = Scraper()
            scraper.start()

    def set_arguments(self, parser):
        parser.add_argument('start')
