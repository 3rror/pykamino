import os
from os import path
from shutil import copy

import toml
from appdirs import user_config_dir

filename = 'pykamino.toml'
user_path = path.join(user_config_dir('pykamino'), filename)

if not path.exists(user_path):
    os.makedirs(user_config_dir('pykamino'), exist_ok=True)
    orig_file = copy(path.join(path.dirname(__file__), filename), user_path)

# At the moment, this is just a dict. In future, it may become a fancy class
config = toml.load(user_path)
