# Pykamino

Pykamino is a all-in-one tool to both retrieve data and extract useful __features__ from CoinBasePro API. These __features__ can then be used to train a neural network to predict crypto's market movements.

## Installation

Being a private project, this package is not hosted publicly, e.g. on PyPi.org. To install it from a local directory, just issue:
```python
pip install pykamino --no-index --find-links '/path/to/directory'
```
This command will install `pykamino` along with its minimal dependencies (internet connection required if none of them is in a local repository).  
Do note that in order to function, `pykamino` has to connect to a relational database. Since we cannot know a priori which DBMS the end user runs, database drivers are considered optional dependencies, so it's up to the end user to install the needed database driver. Currently, we support PostgreSQL, MariaDB/MySQL and SQLite 3.

## Usage

Todo

## License

Pykamino is released under the [Apache License 2.0](https://opensource.org/licenses/Apache-2.0).
