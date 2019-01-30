from os import path

from appdirs import user_config_dir

import toml

# At the moment, this is just a dict. In future, it may become a fancy class
config = toml.load(path.join(user_config_dir('pykamino'), 'pykamino.toml'))
