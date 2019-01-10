from appdirs import user_config_dir
from setuptools import setup
import pykamino

setup(
    name="pykamino",
    version=pykamino.__version__,
    description="All-In-One tool for project Naboo",
    url="https://gitlab-rbl.unipv.it/fforni/pykamino",
    author="Fabio Forni, Gianluca Andreotti",
    author_email="fabio.forni01@universitadipavia.it, gianluca.andreotti01@universitadipavia.it",
    license="Apache 2",
    packages=["pykamino"],
    zip_safe=True,
    scripts=["bin/pykamino"],
    data_files=[(user_config_dir('pykamino'), [
                 'pykamino/_config/pykamino.toml'])],
    install_requires=['appdirs~=1.4.0',
                      'cbpro~=1.1.0',
                      'pandas~=0.23.0',
                      'peewee~=3.8.0',
                      'service~=0.5.0',
                      'toml~=0.10.0',
                      ]
)
