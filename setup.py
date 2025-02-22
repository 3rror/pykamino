from setuptools import setup, find_packages

import pykamino

requirements = open('requirements.txt').read().splitlines()

setup(
    name=pykamino.__name__,
    version=pykamino.__version__,
    description='All-In-One tool for project Naboo',
    url='https://gitlab-rbl.unipv.it/fforni/pykamino',
    author='Fabio Forni, Gianluca Andreotti',
    author_email='fabio.forni01@universitadipavia.it, gianluca.andreotti01@universitadipavia.it',
    license='Apache 2',
    packages=find_packages(exclude=['tests']),
    include_package_data=True,
    zip_safe=True,
    scripts=['bin/pykamino'],
    python_requires='>=3.6.0',
    install_requires=requirements,
    extras_require={
        'postgresql': ['psycopg2~=2.8.0']
    },
)
