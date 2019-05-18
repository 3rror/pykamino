from appdirs import user_config_dir
from setuptools import find_namespace_packages, setup

with open('requirements.txt', 'r') as req:
    requirements = req.read().splitlines()

setup(
    name='pykamino',
    version='0.9.0',
    description='All-In-One tool for project Naboo',
    url='https://gitlab-rbl.unipv.it/fforni/pykamino',
    author='Fabio Forni, Gianluca Andreotti',
    author_email='fabio.forni01@universitadipavia.it, gianluca.andreotti01@universitadipavia.it',
    license='Apache 2',
    # This is a namespace project as it lacks of __init__.py files
    packages=find_namespace_packages(),
    zip_safe=True,
    test_suite='tests',
    scripts=['bin/pykamino'],
    data_files=[(user_config_dir('pykamino'), [
                 'pykamino/data/pykamino.toml'])],
    install_requires=requirements
)
