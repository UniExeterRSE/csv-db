# csv-db

A Python package for working with a simple csv file database.
-----

**Table of Contents**

- [Installation](#installation)
  - [Updating](#updating)
- [Licence](#licence)


## Installation

_csv-db_ is not yet available publicly, so you'll need to clone the project's repository
and do a
[local installation with pip](https://pip.pypa.io/en/stable/topics/local-project-installs/)
(into a virtual environment of your choice).

To clone the latest release version:

```sh
git clone https://github.com/UniExeterRSE/csv-db.git
```

Or, clone the latest development version from the `dev` branch:

```sh
git clone --branch dev https://github.com/UniExeterRSE/csv-db.git
```

Install with `pip` locally (after having activated your desired virtual environment):

```sh
pip install path/to/csv-db/

# Or, in editable mode:
pip install -e path/to/csv-db/
```

> Tip: make sure to give a path-like source to ensure `pip` doesn't try to look for the
> package on PyPI. For example, use `pip intall ./csv-db` rather than `pip install csv-db`.


### Updating

To update, pull in the latest changes with Git and then update your installation with
`pip`:

```sh
cd path/to/csv-db
git pull origin
pip install --upgrade .  # not needed if you did an editable installation
```


## Licence

`csv-db` is copyright University of Exeter and distributed under the terms of
the [BSD-3-Clause](https://opensource.org/license/bsd-3-clause/) licence. See
the [licence file](./LICENCE.txt) for full details.
