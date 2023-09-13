# DEPRECATED

Please use the monorepo at https://github.com/deephaven/deephaven-plugins

Custom implementation built on top of plotly express to make it compatible with deephaven tables.

## Build

To create your build / development environment:

```sh
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip setuptools
pip install build deephaven-plugin plotly
```

To build:

```sh
python -m build --wheel
```

The wheel is stored in `dist/`. 

To test within [deephaven-core](https://github.com/deephaven/deephaven-core), note where this wheel is stored (using `pwd`, for example).
Then, follow the directions in the [deephaven-js-plugins](https://github.com/deephaven/deephaven-js-plugins) repo.

To unit test, run the following command from the root of the repo:
```sh
tox -e py
```

## Usage
Once you have the plugin installed and the server started, the recommended way to import the package mirrors plotly express:
```python
import deephaven.plot.express as dx
```

Then, you can create a table (or use an existing one) and start plotting
```python
from deephaven.column import int_col, string_col
import deephaven.plot.express as dx
from deephaven import new_table

source = new_table([
    string_col("Categories", ["A", "B", "C"]),
    int_col("Values", [1, 3, 5]),
])

fig = dx.bar(table=source, x="Categories", y="Values")
```
