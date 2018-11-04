from setuptools import setup

setup(
    name="pykamino",
    version="0.1",
    description="All-In-One tool for project Naboo",
    url="https://gitlab-rbl.unipv.it/fforni/pykamino",
    author="Fabio Forni, Gianluca Andreotti",
    author_email="fabio.forni01@universitadipavia.it, gianluca.andreotti01@universitadipavia.it",
    license="Apache 2",
    packages=["pykamino"],
    zip_safe=True,
    scripts=["bin/pykamino"],
)
