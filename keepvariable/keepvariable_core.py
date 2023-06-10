import ast
import datetime
import inspect
import json
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Optional

import numpy as np
import pandas as pd
import redis
from redis.commands.search.query import Query
from redis.lock import Lock as RedisLock


def get_definition(jump_frames, *args, **kwargs):
    """Return the definition of a function or a class from inside."""
    frame = inspect.currentframe()
    frame = inspect.getouterframes(frame)[jump_frames]
    try:
        string = inspect.getframeinfo(frame[0]).code_context[0].strip()
    except TypeError as e:
        print("Warning: Keepvariable was not correctly executed", e)
        string = ""
    return string


def analyze_definition(string):
    args = string[string.find("(") + 1 : -1].split(",")
    inputs = []
    for i in args:
        if i.find("=") != -1:
            inputs.append(i.split("=")[1].strip())
        else:
            inputs.append(i)
    try:
        keyword = string.split("=")[1].split("(")[0].strip()
        varname = string.split("=")[0].strip()
    except:
        keyword = ""
        varname = ""
    return (varname, keyword, inputs)


kept_variables = {}


class Var:
    def __new__(cls, var):
        definition = get_definition(2, var)
        varname, keyword, inputs = analyze_definition(definition)
        joined_inputs = ",".join(inputs)
        try:
            # not use ast.literal_eval -> wrong handling of strings for this use case
            kept_variables[varname] = eval(joined_inputs)
        except NameError:
            kept_variables[varname] = var
        return var


class VarSafe:
    def __new__(cls, var, varname, inputs):
        """
        var=variable
        varname=string of var.__name__
        inputs=parameters in bracket.

        Example - difference to kv.Var:
        db_details_list=kv.Var(db_details_list)
        db_details_list=kv.VarSafe(db_details_list,"db_details_list","db_details_list")
        """
        # definition=get_definition(2,var)
        # varname,keyword,inputs=analyze_definition(definition)
        joined_inputs = ",".join(inputs)
        try:
            # not use ast.literal_eval -> wrong handling of strings for this use case
            kept_variables[varname] = eval(joined_inputs)
        except NameError:
            kept_variables[varname] = var
        return var


def save_variables(variables, filename="vars.kpv"):
    with open(filename, "w+", encoding="utf8", errors="ignore") as file:  # errors ignore dirty way - might be improved
        # try:
        file.write(str(variables))  # .encode("utf-8")
        # except UnicodeEncodeError:
        # print("")
        # pass


def load_variable_safe(filename="vars.kpv", varname="varname"):
    with open(filename, encoding="utf8", errors="ignore") as file:  # errors ignore dirty way - might be improved
        rows = file.readlines()
    variable_dict = ast.literal_eval(rows[0])
    this_variable = variable_dict[varname]
    return this_variable


def load_variable(filename="vars.kpv"):
    definition = get_definition(2)
    varname, keyword, inputs = analyze_definition(definition)
    this_variable = load_variable_safe(filename=filename, varname=varname)
    return this_variable


def load_variables(filename="vars.kpv"):
    with open(filename, encoding="utf8", errors="ignore") as file:  # errors ignore dirty way - might be improved
        rows = file.readlines()
    variable_dict = ast.literal_eval(rows[0])
    return variable_dict


class RefList:
    """This object type serves for enabling grouping lists of objects (e.g. visible/draggable) with common attribute in one list which is always up to date."""

    def __init__(self, elements=None, referenced_lists=None):
        if elements is None:
            elements = []

        self.elements = elements
        self.referenced_lists = referenced_lists
        self.embedded_in_lists = []
        if self.referenced_lists is not None:
            self.elements = []
            for i, magic_list in enumerate(self.referenced_lists):
                for item in magic_list.elements:
                    self.elements.append(item)
                self.referenced_lists[i].embedded_in_lists.append(self)

    def append(self, obj):
        self.elements.append(obj)
        for i, list1 in enumerate(self.embedded_in_lists):
            self.embedded_in_lists[i].elements = []
            for j in range(len(list1.referenced_lists)):
                self.embedded_in_lists[i].elements += list1.referenced_lists[j].elements

    def pop(self, index):
        self.elements.pop(index)
        for _i, item in enumerate(self.embedded_in_lists):
            item.elements.pop(index)

    def __str__(self):
        return str(self.elements)


class AbstractKeepVariableServer(ABC):
    def parse_saved_value(self, value, additional_params: Optional[dict] = None):
        """
        Parse enterted value to json format. Certain special type values are serialized (DFs, datetimes, functions, classes).

        :param value: Entered value of any type (not all types can get serialized and stored however!)
        :type value: Any
        :param additional_params: Additional parameters used for serialization, e.g. for a function variable it's
        code must be passed somehow --> additional_params = {'code': <function_code>}. Defaults to {}.
        :type additional_params: Optional[dict], optional
        :return: Jsonified value
        :rtype: Any
        """
        if additional_params is None:
            additional_params = {}

        if isinstance(value, list) or isinstance(value, bool) or isinstance(value, dict):
            value = json.dumps(value)
        elif isinstance(value, pd.DataFrame):
            data = value.values.tolist()
            columns = list(value.columns)
            final_data = {
                "columns": columns,
                "data": data,
                "object_type": "pd.DataFrame",
            }
            print(final_data)
            value = json.dumps(final_data)
        elif isinstance(value, np.ndarray):
            data = value.tolist()
            final_data = {"data": data, "object_type": "np.ndarray"}
            value = json.dumps(final_data)

        elif isinstance(value, datetime.datetime):
            data = value.strftime("%Y-%m-%d %H:%M:%S")
            final_data = {"data": data, "object_type": "datetime.datetime"}
            value = json.dumps(final_data)
        elif inspect.isfunction(value):
            code = additional_params.get("code")
            value = {"code": code, "object_type": "function"}
            value = json.dumps(code)
        elif inspect.isclass(value):
            code = additional_params.get("code")
            value = {"code": code, "object_type": "class"}
            value = json.dumps(code)

        return value

    def decode_loaded_value(self, value):
        """
        Decode value stored in redis into it's initial value.
        For functions and classes only their code is returned --> they need to be evaluated afterwards!!!.

        :param value: Variable value from redis
        :type value: Any
        :return: Parsed variable value
        :rtype: Any
        """
        try:
            value = json.loads(value)
            if "object_type" in value and isinstance(value, dict):
                if value["object_type"] == "pd.DataFrame":
                    df = pd.DataFrame(value["data"], columns=value["columns"])
                    return df
                elif value["object_type"] == "np.ndarray":
                    array = pd.DataFrame(
                        value["data"]
                    ).values  # to ensure 64bit values in array
                    return array

                elif value["object_type"] == "datetime.datetime":
                    datetime_value = datetime.datetime.strptime(
                        value["data"], "%Y-%m-%d %H:%M:%S"
                    )
                    return datetime_value
                elif (
                    value["object_type"] == "function"
                    or value["object_type"] == "class"
                ):
                    return value["code"]
            return value
        except json.JSONDecodeError:  # if type is str, it fails to decode
            return value

    @abstractmethod
    def lock(self, *args, **kwargs) -> RedisLock:
        pass

    @abstractmethod
    def set(self, key: str, value, additional_params: Optional[dict] = None):
        pass

    @abstractmethod
    def get(self, key: str):
        pass

    @abstractmethod
    def json_mset(self, name: str, params: dict) -> None:
        """Set multiple keys in json document - explanations are in abstract subclasses docstrings."""
        pass

    @abstractmethod
    def query(
        self, query_params: dict, *args, field_to_sort_by: Optional[str] = None, asc=True,
    ) -> list[str]:
        """Query KeepVariable store - explanations are in abstract subclasses docstrings."""
        pass

    # Implemented, but not currently used
    @abstractmethod
    def arrlen(self, name: str, path: str) -> Optional[int]:
        """
        Return length of the specified array in JSON document.

        :param name: key under which a JSON document is stored
        :type name: str
        :param path: Redis JSON path string e.g. "job.nodes[2].status"
        :type path: str
        :return: Size of the array
        :rtype: Optional[int]
        """
        pass

    @abstractmethod
    def arrappend(self, name: str, path: str, objects: Sequence) -> Optional[int]:
        """
        Append to the specified array in JSON document.

        :param name: key under which a JSON document is storedobjects: list
        :type name: str
        :param path: Redis JSON path string e.g. "job.nodes[2].status"
        :type path: str
        :return: Final size of the array
        :rtype: Optional[int]
        """
        pass


class KeepVariableDummyRedisServer(AbstractKeepVariableServer):
    def __init__(self, host="localhost"):
        self.host = host
        self.storage = {}

    def lock(self, *args, **kwargs) -> RedisLock:
        """
        Create a fake lock, which does nothing but allows KeepVariableDummyRedisServer
        to conform to the interface.
        """
        class DummyLock:
            def acquire(self):
                return True

            def release(self):
                pass

        return DummyLock()

    def set(self, key, value, additional_params: Optional[dict] = None):
        if additional_params is None:
            additional_params = {}

        value = self.parse_saved_value(value, additional_params)
        self.storage[key] = value
        return {key: value}

    def get(self, key):
        value = self.storage.get(key)
        decoded_value = self.decode_loaded_value(value)
        return decoded_value

    def json_mset(self, name: str, params: dict, *args, **kwargs) -> None:
        """
        Set multiple keys in a JSON document.

        :param name: key under which a JSON document is stored
        :type name: str
        :param params: collection of arguments where keys are JSON Paths and values are values to set
        Keys are Redis Path strings, allowing access to specific elements withing JSON document
        More info: https://redis.io/docs/stack/json/path/
        :type params: dict

        e.g.
        params = {"$.is_saved"=true, "$.status"=SomeEnum.COMPLETED.value}
        """
        for json_xpath, value in params.items():
            element, last_element, last_index = self._extract_object_from_path(
                name, json_xpath
            )
            if last_index:
                element[last_element][last_index] = value
            else:
                element[last_element] = value

    def query(
        self, query_params: dict, *args, field_to_sort_by: Optional[str] = None,
        asc=True, **kwargs,
    ) -> list[dict]:
        """
        Simplified alternative to RedisSearch. Allows to search and sort by values of specified fields.

        :param query_params: attribute name to value mapping, e.g. {'status': 'QUEUED', ...}
        :type query_params: dict
        :param sort_by_name: attribute name by which results should be sorted
        :type sort_by_name: str
        :return:
        :rtype: Optional[dict]
        """
        found_jobs = self.storage.values()
        for field, value in query_params.items():
            found_jobs = [job for job in found_jobs if job[field] == value]
        if field_to_sort_by:
            found_jobs = sorted(found_jobs, key=lambda x: x[field_to_sort_by])
        if not asc:
            found_jobs.reverse()

        return found_jobs

    def arrlen(self, name: str, path: str) -> Optional[int]:
        try:
            element, last_element, last_index = self._extract_object_from_path(
                name, path
            )
            if last_index:
                return len(element[last_element][last_index])
            else:
                return len(element[last_element])
        except KeyError:  # path points to a nonexistent object
            return None

    def arrappend(self, name: str, path: str, objects: Sequence) -> Optional[int]:
        try:
            element, last_element, last_index = self._extract_object_from_path(
                name, path
            )
            if last_index:
                for obj in objects:
                    element[last_element][last_index].append(obj)
                return len(element[last_element][last_index])
            else:
                for obj in objects:
                    element[last_element].append(obj)
                return len(element[last_element])
        except KeyError:  # path points to a nonexistent object
            return None

    def _extract_object_from_path(
        self, name: str, path: str
    ) -> tuple[Any, str, Optional[int]]:
        """
        Recursively traverses a JSON document under 'name' to access the object defined by the 'path' argument.

        :param name: key under which a JSON document is stored
        :type name: str
        :param path: Redis JSON path string e.g. "job.nodes[2].status"
        :type path: str
        :return: tuple[referenced object, key, index]
        :rtype: tuple[dict, str, int]

        As Python does not have pointers, we have to trick it by passing a reference.
        We're returning a reference to the object 1 level above the final one dictated by the 'path' argument
        Additionally, we are returning the key to the last value we want to access
        And the index if that value is actually a list, so we can access that list
        We can use these return values to access object described by 'path' in the original function

        tuple[referenced_object, key, index] --> referenced_object[key][index] = ...
        """
        # Parsing sequence from 'path' string
        # name = "cache"
        # path = "$.A.B[2].C.D[5]"
        # elements = ["$", "A", "B[2]", "C", "D[5]"]
        # element_list = ["cache", "A", "B", "C", "D"]
        # index_list = [None, None, 2, None, 5]
        # Then iterate over pairs of both lists and recurrently dive into nested objects

        # Parsing logic
        elements = path.split(".")
        pattern = r"\[(\d+)\]"  # Find any "[*]" group where * is one or more digits
        element_list: list[str] = []
        index_list: list[Optional[int]] = []
        for element in elements:
            if match := re.search(pattern, element):
                group = match.group()
                index_list.append(int(group[1:-1]))  # transform "[2]" -> 2
                element_list.append(element.split("[")[0])  # transform "B[2]" -> "B"
            else:
                index_list.append(None)
                element_list.append(element)

        root = element_list.index("$")
        element_list[root] = name

        # Traversing logic
        nested_object = self.storage
        for element, index in zip(element_list[:-1], index_list[:-1]):
            if index is not None:
                nested_object = nested_object[element][index]
            else:
                nested_object = nested_object[element]

        return nested_object, element_list[-1], index_list[-1]


class KeepVariableRedisServer(AbstractKeepVariableServer):
    def __init__(self, host="localhost", port=6379, password=None):
        self.host = host
        self.port = port
        self.password = password

        self.redis = redis.Redis(
            host=self.host,
            port=self.port,
            db=0,
            password=self.password,
            decode_responses=True,
            charset="utf-8",
        )

    @property
    def kept_variables(self):
        return self._kept_variables

    @kept_variables.setter
    def kept_variables(self, kept_variables):
        return self._kept_variables

    def lock(self, *args, **kwargs):
        """Wrap Redis Lock object. Inspect wrapped object to investigate it's signature."""
        return self.redis.lock(*args, **kwargs)

    def set(self, key: str, value: str, additional_params: Optional[dict] = None):
        if additional_params is None:
            additional_params = {}

        value = self.parse_saved_value(value, additional_params)
        result = self.redis.set(key, value)
        return result

    def get(self, key: str) -> Optional[Any]:
        value = self.redis.get(key)

        if value is None:
            return value

        decoded_value = self.decode_loaded_value(value)
        return decoded_value

    def json_mset(self, name: str, params: dict[str, Any], transaction=True) -> None:
        """
        Set multiple keys in Redis JSON document. This method uses RedisJSON.

        :param name: Key of the Redis variable
        :type name: str
        :param params: dict where keys are JSON Paths and values are values to set
        Keys are Redis Path strings, allowing access to specific elements withing JSON document
        More info: https://redis.io/docs/stack/json/path/
        :type params: dict
        :param transaction: True if all operations should be done in a single transaction, defaults to True
        :type transaction: bool, optional

        e.g.
        params = {"$.is_saved"=true, "$.status"=SomeEnum.COMPLETED.value}
        """
        with self.redis.pipeline(transaction=transaction) as pipe:
            for json_xpath, value in params.items():
                pipe.json().set(name, json_xpath, value)
            pipe.execute()

    def query(
        self, query_params: dict, index_name: str, *args,
        field_to_sort_by: Optional[str] = None, asc=True, **kwargs
    ) -> list[str]:
        """
        Simplified wrapper to RedisSearch. Allows to search and sort by a value of Redis TAG fields.
        Simplistic on purpose, to avoid bloat. Additional functionality should be added in case of need.

        :param query_params: attribute name to value mapping, e.g. {'status': 'QUEUED', ...}
        :type query_params: dict
        :param index_name: name of the Redis index used for querying
        :type index_name: str
        :param field_to_sort_by: attribute name by which results should be sorted, defaults to None
        :type field_to_sort_by: Optional[str], optional
        :param asc: True if sort in ascending order, defaults to True
        :type asc: bool, optional
        :return: list of found document jsons
        :rtype: list[str]
        """
        tag_query_template = "@{field}:{{{value}}}"
        final_query = ""

        for field, value in query_params.items():
            final_query += tag_query_template.format(field=field, value=value)

        query_object = Query(final_query)
        if field_to_sort_by:
            query_object.sort_by(field_to_sort_by, asc=asc)
        job_docs: list = self.redis.ft(index_name).search(query_object).docs

        return [job_doc.json for job_doc in job_docs]

    def arrlen(self, name: str, path: str) -> Optional[int]:
        return self.redis.json().arrlen(name, path).pop()

    def arrappend(self, name: str, path: str, objects: Sequence) -> Optional[int]:
        return self.redis.json().arrappend(name, path, *objects).pop()
