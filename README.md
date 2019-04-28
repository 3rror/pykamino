# Pykamino

Pykamino is a all-in-one tool to both retrieve data and extract useful __features__ from CoinBasePro API. These __features__ can then be used to train a neural network to predict crypto's market movements.\
Data is downloaded in real time from the [Coinbase Pro](https://pro.coinbase.com/) exchange platform.

## Installation

Being a private project, this package is not hosted publicly, e.g. on PyPi.org. To install it from a local directory, just issue (root rights may be needed):

```bash
pip3 install pykamino '/path/to/directory'
```

This command will install `pykamino` along with its minimal dependencies (internet connection required if none of them is in a local repository).\
Do note that in order to function, `pykamino` has to connect to a relational database. Since we cannot know a priori which DBMS the end user runs, database drivers are considered optional dependencies, so it's up to the end user to install the needed database driver. Currently, we support PostgreSQL, MariaDB/MySQL and SQLite 3.

## Configuration

After the installation, you'll find a file named `pykamino.toml` inside your config directory: `$XDG_CONFIG_HOME/pykamino` on Linux, `~/Library/Application Support/pykamino/` on macOS. Edit it according to your database and products you want to consider.

## Usage

### Scraper

`pykamino scraper run` to start the downloading service and `pykamino scraper stop` to stop the service. Optionally, when running the service you can pass `-b BUFFER` where `BUFFER` is how many valid Coinbase messages you want to keep in memory before saving them into the database.

### Features

To extract features, run `pykamino features category start end resolution`.

- **category**: choose from "orders", "trades" or "all"
- **start**: date and time in ISO8601 format (e.g. '2010-01-01 11:00:00.00'. Microseconds are optional)
- **end**: ditto
- **resolution**: size of the advancing time window, using [pandas' syntax](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#timeseries-offset-aliases) (e.g. '10min', '2h40min')

## License

Pykamino is released under the [Apache License 2.0](https://opensource.org/licenses/Apache-2.0).
