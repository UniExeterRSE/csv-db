# csv-db

A Python package for working with a simple csv file database.
-----

**Table of Contents**

- [Installation](#installation)
  - [Updating](#updating)
- [Tutorial](#tutorial)
  - [Initialising the database](#initialising-the-database)
  - [Creating new records](#creating-new-records)
  - [Retrieving records](#retrieving-records)
  - [Updating records](#updating-records)
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


## Tutorial

### Initialising the database

The main export from _csv-db_ is the `CsvDB` class. We use it to interact with a new
csv database file containing records of favourite animals.

```python
>>> from csv_db import CsvDB
>>> db = CsvDB("./database.csv", fields=["ID", "Name", "Age", "Species"])
```

The database file isn't created until a record is added to the database for the first
time. _Records_ are a key concept in _csv-db_: they are simply dictionaries with string
keys which correspond to the field names. Each such dictionary represents a single entry
in the database, corresponding to a row of the underlying csv file.


### Creating new records

Let's go ahead and create a first record. Notice how the values in our input dictionary
don't have to be strings: they will be converted to strings under-the-hood before being
written to the database.

```python
>>> record = {"ID": "1", "Name": "Fido", "Age": 3, "Species": "Dog"}
>>> db.create(record)
```

This will create a new csv file `database.csv`. If we take a look inside, we see the
new record:

```txt
ID,Name,Age,Species
1,Fido,3,Dog

```

Before continuing, we'll add a few more records:

```python
>>> db.create({"ID": "2", "Name": "Puss in Boots", "Age": 22, "Species": "Cat"})
>>> db.create({"ID": "3", "Name": "Lassie", "Age": 6, "Species": "Dog"})
>>> db.create({"ID": "4", "Name": "Mickey", "Age": 95, "Species": "Mouse"})
>>> db.create({"ID": "5", "Name": "Santa's Little Helper", "Age": None, "Species": "Dog"})
```

### Retrieving records

We can retrieve the entry with `ID` equal to `4` by using the `retrieve()` method:

```python
>>> db.retrieve(field="ID", value="4")
{'ID': '4', 'Name': 'Mickey', 'Age': '95', 'Species': 'Mouse'}
```

Notice how the returned dictionary has all values being strings: no type conversion
is performed when reading records from the database.

If we want to view all records in the database, we can use the `query()` method:

```python
>>> for record in db.query():
...     print(record)
... 
{'ID': '1', 'Name': 'Fido', 'Age': '3', 'Species': 'Dog'}
{'ID': '2', 'Name': 'Puss in Boots', 'Age': '22', 'Species': 'Cat'}
{'ID': '3', 'Name': 'Lassie', 'Age': '6', 'Species': 'Dog'}
{'ID': '4', 'Name': 'Mickey', 'Age': '95', 'Species': 'Mouse'}
{'ID': '5', 'Name': "Santa's Little Helper", 'Age': '', 'Species': 'Dog'}
```

Alternatively, we can filter the records using a suitable predicate function, which
maps a record to a boolean. For example, to get the records that are for animals that
are not dogs, we could run the following:

```python
>>> not_dogs = db.query(lambda x: not x["Species"] == "Dog")
>>> for record in not_dogs:
...     print(record)
... 
{'ID': '2', 'Name': 'Puss in Boots', 'Age': '22', 'Species': 'Cat'}
{'ID': '4', 'Name': 'Mickey', 'Age': '95', 'Species': 'Mouse'}
```


### Updating records

If we want to update a record, we can use the `update()` method. This works similar to
`retrieve()`, except it also takes the new record to replace the old one with. For
example, we could update the record for Santa's Little Helper to give him an age:

```python
>>> santas_little_helper = db.retrieve(field="ID", value="5")
>>> santas_little_helper["Age"] = 4
>>> db.update(field="ID", value="5", record=santas_little_helper)
```

We can see that the record has been updated:
```python
>>> db.retrieve(field="ID", value="5")
{'ID': '5', 'Name': "Santa's Little Helper", 'Age': '4', 'Species': 'Dog'}
```

## Licence

`csv-db` is copyright University of Exeter and distributed under the terms of
the [BSD-3-Clause](https://opensource.org/license/bsd-3-clause/) licence. See
the [licence file](./LICENCE.txt) for full details.
