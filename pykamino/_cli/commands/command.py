from abc import ABCMeta, abstractmethod


class Command(metaclass=ABCMeta):
    def __init__(self, name=None, description=None):
        self.name = name or self.__class__.__name__.lower()
        self.description = description

    def add_to_parser(self, parser):
        self.parser = parser.add_parser(
            self.name, description=self.description)
        self.set_arguments(self.parser)
        self.parser.set_defaults(action=self)

    @abstractmethod
    def __call__(self, args):
        """
        Define the operations that the command must perform.
        """
        pass

    @abstractmethod
    def set_arguments(self, parser):
        pass
