# import required modules
import argparse
import importlib
import inspect
import pydoc
import os
import pathlib
import sys
import types

import docspec
import docspec_python


# ==> The list of files in the Node working directory

modules = docspec_python.load_python_modules(
    modules=["deephaven.plot.express"], search_path=["./src"]
)

print(modules)

dump = None
for module in modules:
    dump = docspec.dump_module(module)

print(dump)
