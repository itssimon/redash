import datetime
import importlib
import logging
import math
import sys
from typing import Any, Dict, List, Union

import numpy as np
import pandas as pd
import scipy
from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from redash import models
from RestrictedPython import compile_restricted
from RestrictedPython.Guards import safe_builtins


logger = logging.getLogger(__name__)


class CustomPrint(object):
    """CustomPrint redirect "print" calls to be sent as "log" on the result object."""

    def __init__(self):
        self.enabled = True
        self.lines = []

    def write(self, text):
        if self.enabled:
            if text and text.strip():
                self.lines.append(text)

    def enable(self):
        self.enabled = True

    def disable(self):
        self.enabled = False

    def __call__(self, *args):
         return self

    def _call_print(self, *args, **kwargs):
        print(*args, file=self)


class Python(BaseQueryRunner):
    should_annotate_query = False

    safe_builtins = (
        "sorted",
        "reversed",
        "map",
        "any",
        "all",
        "slice",
        "filter",
        "len",
        "next",
        "enumerate",
        "sum",
        "abs",
        "min",
        "max",
        "round",
        "divmod",
        "str",
        "int",
        "float",
        "complex",
        "tuple",
        "set",
        "list",
        "dict",
        "bool",
        "zip",
    )

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "allowedImportModules": {
                    "type": "string",
                    "title": "Modules to import prior to running the script",
                },
                "additionalModulesPaths": {"type": "string"},
            },
        }

    @classmethod
    def enabled(cls):
        return True

    def __init__(self, configuration):
        super(Python, self).__init__(configuration)

        self.syntax = "python"

        self._allowed_modules = {
            "math": math,
            "pandas": pd,
            "numpy": np,
            "scipy": scipy,
        }
        self._script_locals = {"result": pd.DataFrame()}
        self._enable_print_log = True
        self._custom_print = CustomPrint()

        if self.configuration.get("allowedImportModules", None):
            for item in self.configuration["allowedImportModules"].split(","):
                item = item.strip()
                if item not in self._allowed_modules:
                    self._allowed_modules[item] = None

        if self.configuration.get("additionalModulesPaths", None):
            for p in self.configuration["additionalModulesPaths"].split(","):
                if p not in sys.path:
                    sys.path.append(p)

    def custom_import(self, name, globals=None, locals=None, fromlist=(), level=0):
        if name in self._allowed_modules:
            m = None
            if self._allowed_modules[name] is None:
                m = importlib.import_module(name)
                self._allowed_modules[name] = m
            else:
                m = self._allowed_modules[name]

            return m

        raise Exception(
            "'{0}' is not configured as a supported import module".format(name)
        )

    @staticmethod
    def custom_write(obj):
        """
        Custom hooks which controls the way objects/lists/tuples/dicts behave in
        RestrictedPython
        """
        return obj

    @staticmethod
    def custom_get_item(obj, key):
        return obj[key]

    @staticmethod
    def custom_get_iter(obj):
        return iter(obj)

    @staticmethod
    def get_source_schema(data_source_name_or_id: Union[str, int]):
        """Get schema from specific data source.

        :param data_source_name_or_id: string|integer: Name or ID of the data source
        :return:
        """
        try:
            if type(data_source_name_or_id) == int:
                data_source = models.DataSource.get_by_id(data_source_name_or_id)
            else:
                data_source = models.DataSource.get_by_name(data_source_name_or_id)
        except models.NoResultFound:
            raise Exception("Wrong data source name/id: %s." % data_source_name_or_id)
        schema = data_source.query_runner.get_schema()
        return schema

    @staticmethod
    def execute_query(data_source_name_or_id: Union[str, int], query: str) -> pd.DataFrame:
        """Run query from specific data source.

        Parameters:
        :data_source_name_or_id string|integer: Name or ID of the data source
        :query string: Query to run
        """
        try:
            if type(data_source_name_or_id) == int:
                data_source = models.DataSource.get_by_id(data_source_name_or_id)
            else:
                data_source = models.DataSource.get_by_name(data_source_name_or_id)
        except models.NoResultFound:
            raise Exception("Wrong data source name/id: %s." % data_source_name_or_id)

        # TODO: pass the user here...
        data, error = data_source.query_runner.run_query(query, None)
        if error is not None:
            raise Exception(error)

        # TODO: allow avoiding the JSON dumps/loads in same process
        return Python.df_from_result(json_loads(data))

    @staticmethod
    def get_query_result(query_id: int) -> pd.DataFrame:
        """Get result of an existing query.

        Parameters:
        :query_id integer: ID of existing query
        """
        try:
            query = models.Query.get_by_id(query_id)
        except models.NoResultFound:
            raise Exception("Query id %s does not exist." % query_id)

        if query.latest_query_data is None or query.latest_query_data.data is None:
            raise Exception("Query does not have results yet.")

        return Python.df_from_result(query.latest_query_data.data)

    @staticmethod
    def df_from_result(result: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
        df = pd.DataFrame.from_records(result["rows"])
        column_types = {c["name"]: c["type"] for c in result["columns"]}

        for c in df.columns:
            t = column_types[c]
            if t == TYPE_DATETIME or t == TYPE_DATE:
                df[c] = pd.to_datetime(df[c])
            elif t == TYPE_BOOLEAN:
                df[c] = df[c].astype("boolean")
            elif t == TYPE_INTEGER:
                df[c] = df[c].astype("Int64")
            elif t == TYPE_STRING:
                df[c] = df[c].astype("string")

        return df

    @staticmethod
    def result_from_df(df: pd.DataFrame) -> Dict[str, List[Dict[str, Any]]]:
        df = df.copy()
        columns = []

        for c, t in df.dtypes.iteritems():
            t = str(t).lower()
            column_type = "unknown"
            if t == "object" and df[c].apply(lambda x: isinstance(x, bool) or pd.isna(x)).all():
                df[c] = df[c].astype("boolean")
                column_type = TYPE_BOOLEAN
            elif t == "object" and df[c].apply(lambda x: isinstance(x, datetime.date) or pd.isna(x)).all():
                column_type = TYPE_DATE
            elif t.startswith("int"):
                df[c] = df[c].astype("Int64")
                column_type = TYPE_INTEGER
            elif t.startswith("uint"):
                column_type = TYPE_INTEGER
            elif t.startswith("float") and df[c].apply(lambda x: x.is_integer() or pd.isna(x)).all():
                df[c] = df[c].astype("Int64")
                column_type = TYPE_INTEGER
            elif t.startswith("float"):
                column_type = TYPE_FLOAT
            elif t.startswith("bool"):
                column_type = TYPE_BOOLEAN
            elif t.startswith("datetime"):
                column_type = TYPE_DATETIME
            elif t.startswith("timedelta"):
                df[c] = df[c].dt.total_seconds()
                column_type = TYPE_FLOAT
            elif t.startswith("period"):
                df[c] = df[c].dt.to_timestamp()
                column_type = TYPE_DATETIME
            else:
                df[c] = df[c].apply(lambda x: str(x) if pd.notna(x) else None).astype("string")
                column_type = TYPE_STRING
            columns.append({"name": str(c), "friendly_name": str(c), "type": column_type})

        def convert_value(v: Any) -> Any:
            if pd.isna(v):
                return None
            elif isinstance(v, np.integer):
                return int(v)
            elif isinstance(v, np.floating):
                return float(v)
            elif isinstance(v, pd.Timestamp):
                return v.to_pydatetime()
            elif isinstance(v, pd.Period):
                return v.to_timestamp().to_pydatetime()
            elif isinstance(v, pd.Interval):
                return str(v)
            return v

        return {
            "columns": columns,
            "rows": [{str(k): convert_value(v) for k, v in r.items()} for r in df.to_dict(orient="records")],
        }

    def get_current_user(self):
        return self._current_user.to_dict()

    def test_connection(self):
        pass

    def run_query(self, query, user):
        self._current_user = user

        try:
            error = None

            code = compile_restricted(query, "<string>", "exec")

            builtins = safe_builtins.copy()
            builtins["_write_"] = self.custom_write
            builtins["__import__"] = self.custom_import
            builtins["_getattr_"] = getattr
            builtins["getattr"] = getattr
            builtins["_setattr_"] = setattr
            builtins["setattr"] = setattr
            builtins["_getitem_"] = self.custom_get_item
            builtins["_getiter_"] = self.custom_get_iter
            builtins["_print_"] = self._custom_print

            # Layer in our own additional set of builtins that we have
            # considered safe.
            for key in self.safe_builtins:
                builtins[key] = __builtins__[key]

            restricted_globals = dict(__builtins__=builtins)
            restricted_globals["get_query_result"] = self.get_query_result
            restricted_globals["get_source_schema"] = self.get_source_schema
            restricted_globals["get_current_user"] = self.get_current_user
            restricted_globals["execute_query"] = self.execute_query
            restricted_globals["disable_print_log"] = self._custom_print.disable
            restricted_globals["enable_print_log"] = self._custom_print.enable

            # Add commonly used imports
            restricted_globals["math"] = math
            restricted_globals["pd"] = pd
            restricted_globals["np"] = np
            restricted_globals["pandas"] = pd
            restricted_globals["numpy"] = np
            restricted_globals["scipy"] = scipy

            # Supported data types
            restricted_globals["TYPE_DATETIME"] = TYPE_DATETIME
            restricted_globals["TYPE_BOOLEAN"] = TYPE_BOOLEAN
            restricted_globals["TYPE_INTEGER"] = TYPE_INTEGER
            restricted_globals["TYPE_STRING"] = TYPE_STRING
            restricted_globals["TYPE_DATE"] = TYPE_DATE
            restricted_globals["TYPE_FLOAT"] = TYPE_FLOAT

            # TODO: Figure out the best way to have a timeout on a script
            #       One option is to use ETA with Celery + timeouts on workers
            #       And replacement of worker process every X requests handled.

            exec(code, restricted_globals, self._script_locals)

            if not isinstance(self._script_locals["result"], pd.DataFrame):
                raise ValueError("result is not a pandas DataFrame")

            result = self.result_from_df(self._script_locals["result"])
            result["log"] = self._custom_print.lines
            json_data = json_dumps(result)
        except Exception as e:
            error = type(e).__name__ + ": " + str(e)
            json_data = None

        return json_data, error


register(Python)
