import argparse, sys, daemon, daemon.pidfile


class GenericInterface:
    def __init__(self, depth=1):
        self.parser = argparse.ArgumentParser(add_help=False, usage=self.usage_banner())
        self.parser.add_argument("command")

        # Directly show help if command is not provided
        if len(sys.argv) == depth:
            self.help()

        # parse_args defaults to [1:] for args, but we need to exclude the rest
        # of the args too, or validation will fail
        args = self.parser.parse_args(sys.argv[depth : depth + 1])

        # We use the dispatch pattern to invoke method with same name, but first
        # we check if the method exists
        if not hasattr(self, args.command):
            self._unrecognized_command(args.command)
        getattr(self, args.command)()

    def _unrecognized_command(self, command):
        print(f"Unrecognized command {command}")
        self.parser.print_usage()
        exit(1)

    def usage_banner(self):
        raise NotImplementedError("Override this method.")

    def options(self):
        raise NotImplementedError("Override this method.")

    def help(self):
        self.parser.print_usage()
        exit(0)


class MainInterface(GenericInterface):
    def usage_banner(self):
        return """\
pykamino <command>

Commands:
    scraper         Save data from CoinBasePro API
    extractor       Extract features from previously saved data
    help            Show this help"""

    def scraper(self):
        ScraperInterface()

    def extractor(self):
        ExtractorInterface()


class ScraperInterface(GenericInterface):
    def __init__(self):
        super().__init__(depth=2)

    def usage_banner(self):
        return """\
pykamino scraper <command>

Commands:
    start           Start the daemon
    pause           Suspend the daemon
    help            Show this help
"""

    def start(self):
        from pykamino.scraper import main as scraper_main

        # TODO: Choose a path to store the PID file
        pid_file_path = "/var/tmp/pykamino_scraper.pid"
        with daemon.DaemonContext(pidfile=daemon.pidfile.PIDLockFile(pid_file_path)):
            scraper_main()

    def pause(self):
        pass


class ExtractorInterface(GenericInterface):
    def __init__(self):
        super().__init__(depth=2)

    # TODO
