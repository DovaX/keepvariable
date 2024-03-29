import ast
import datetime
import inspect
import json
import os
import re
from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any, Optional, Union

import numpy as np
import pandas as pd
import redis
from redis.client import Pipeline as RedisPipeline
from redis.commands.search.query import Query
from redis.lock import Lock as RedisLock

from keepvariable.utils import access_element_by_path


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
    args = string[string.find("(") + 1:-1].split(",")
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
        """var=variable
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
    with open(filename, "w+", encoding="utf8", errors="ignore") as file:
        # errors ignore dirty way - might be improved
        # try:
        file.write(str(variables))  # .encode("utf-8")
        # except UnicodeEncodeError:
        # print("")
        # pass


def load_variable_safe(filename="vars.kpv", varname="varname"):
    with open(filename, encoding="utf8", errors="ignore") as file:
        # errors ignore dirty way - might be improved
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
    with open(filename, encoding="utf8", errors="ignore") as file:
        # errors ignore dirty way - might be improved
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
    def _json_serialize_dataframe(self, df: pd.DataFrame) -> str:
        """Takes a pandas DataFrame and serialized it to a json-like string.
        The function uses pd.DataFrame().to_json() approach so as to handle various variable types with ease (pd.NA, pd.NaT, datetime etc.).

        Example:
        -------
            input: df2 = pd.DataFrame([[1,datetime.datetime.now(),3],[4,5,pd.NA],[pd.NaT,8,None]])
            output: {"columns": [0, 1, 2], "data": [[1, 1685402664424, 3], [4, 5, null], [null, 8, null]], "object_type": "pd.DataFrame", "attrs": {}}'

        Args:
        ----
            df (pd.DataFrame): DataFrame to be serialized

        Returns:
        -------
            str: DataFrame serialized into json-like string
        """
        df_json = df.to_json(orient='split')
        df_as_dict = json.loads(df_json)
        df_as_dict["object_type"] = 'pd.DataFrame'
        df_as_dict["attrs"] = df.attrs
        df_json = json.dumps(df_as_dict)

        return df_json

    def parse_saved_value(self, value, additional_params: Optional[dict] = None):
        """Parse enterted value to json format. Certain special type values are serialized (DFs, datetimes, functions, classes).

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

        if value is None:
            value = json.dumps({"object_type": "NoneType"})  # Redis does not natively support None values
        elif (
            isinstance(value, list) or isinstance(value, bool) or isinstance(value, dict) or
            isinstance(value, int) or isinstance(value, float)
        ):
            value = json.dumps(value)
        elif isinstance(value, pd.DataFrame):
            value = self._json_serialize_dataframe(value)
            # Old implementation
            # TODO: Keep for now, delete in following commits
            # data = value.values.tolist()
            # columns = list(value.columns)
            # final_data = {
            #     "columns": columns,
            #     "data": data,
            #     "object_type": "pd.DataFrame",
            # }
            # print(final_data)
            # value = json.dumps(final_data)
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

    def decode_loaded_value(self,
                            value: str) -> Union[dict, pd.DataFrame, np.ndarray, datetime.datetime]:
        """Decode value stored in redis into it's initial value. For functions and classes only their code is returned --> they need to be evaluated afterwards!!!.

        :param value: Variable value from redis
        :type value: Any
        :return: Parsed variable value
        :rtype: Any
        """
        try:
            value = json.loads(value)
            if isinstance(value, dict) and "object_type" in value:
                if value["object_type"] == "NoneType":
                    return None
                elif value["object_type"] == "pd.DataFrame":
                    df = pd.DataFrame(value["data"], columns=value["columns"])
                    return df
                elif value["object_type"] == "np.ndarray":
                    array = pd.DataFrame(value["data"]).values  # to ensure 64bit values in array
                    return array

                elif value["object_type"] == "datetime.datetime":
                    datetime_value = datetime.datetime.strptime(value["data"], "%Y-%m-%d %H:%M:%S")
                    return datetime_value
                elif (value["object_type"] == "function" or value["object_type"] == "class"):
                    return value["code"]
            return value
        except json.JSONDecodeError:  # if type is str, it fails to decode
            return value

    @abstractmethod
    def lock(self, *args, **kwargs) -> RedisLock:
        pass

    @abstractmethod
    def pipeline(self, *, transaction: bool = True) -> RedisPipeline:
        pass

    @abstractmethod
    def set(
        self, key: str, value, additional_params: Optional[dict] = None, *,
        pipeline: Optional[RedisPipeline] = None
    ):
        pass

    @abstractmethod
    def get(self, key: str) -> Union[dict, pd.DataFrame, np.ndarray, datetime.datetime]:
        pass

    @abstractmethod
    def json_mset(self, name: str, params: dict, *,
                  pipeline: Optional[RedisPipeline] = None) -> Optional[RedisPipeline]:
        """Set multiple keys in json document - explanations are in abstract subclasses docstrings."""
        pass

    @abstractmethod
    def query(
        self, *, text_params: Optional[dict[str, tuple]] = None,
        tag_params: Optional[dict[str, tuple]] = None, field_to_sort_by: Optional[str] = None,
        asc=True, **kwargs
    ) -> dict[str, dict]:
        """Query KeepVariable store - explanations are in abstract subclasses docstrings."""
        pass

    # Implemented, but currently not used
    @abstractmethod
    def arrlen(self, name: str, path: str, *,
               pipeline: Optional[RedisPipeline] = None) -> Union[int, None, RedisPipeline]:
        """Return length of the specified array in JSON document, or pipeline if passed.

        :param name: key under which a JSON document is stored
        :type name: str
        :param path: Redis JSON path string e.g. "job.nodes"
        :type path: str
        :return: Length of the array or pipeline if passed. None if array does not exist.
        :rtype: Union[int, None, RedisPipeline]
        """
        pass

    @abstractmethod
    def arrappend(
        self, name: str, path: str, objects: Iterable, *, pipeline: Optional[RedisPipeline] = None
    ) -> Optional[int]:
        """Append to the specified array in JSON document.

        :param name: key under which a JSON document is storedobjects: list
        :type name: str
        :param path: Redis JSON path string e.g. "job.nodes[2].status"
        :type path: str
        :return: Final size of the array
        :rtype: Optional[int]
        """
        pass

    @abstractmethod
    def scan(self, match_string: str, count: int = 50, type_: Optional[str] = None) -> list[str]:
        """Find saved keys, matching their name with a given glob-style pattern. This command does not block the server, as it is based on a cursor-style iterator.

        :param match_string: string pattern to match keys against, e.g. 'jobs:*'
        :type match_string: str
        :param count: how many rows to fetch in one iteration, defaults to 50
        :type count: int, optional
        :param type_: filter on specified Redis key type, defaults to None
        :type type_: Optional[str], optional
        """
        pass

    @abstractmethod
    def delete(self, *names: str,
               pipeline: Optional[RedisPipeline] = None) -> Union[int, RedisPipeline]:
        """Delete multiple keys - explanations are in abstract subclasses docstrings."""
        pass


class KeepVariableDummyRedisServer(AbstractKeepVariableServer):
    def __init__(self, host="localhost"):
        self.host = host
        self.storage = {}
        
        try:
            if os.path.isfile("kv_storage.json"):
                with open("kv_storage.json") as file:
                    json_string = file.read()
                    json_dict = json.loads(json_string)
                    self.storage = {key: json.dumps(value) for key, value in json_dict.items()}
        except json.decoder.JSONDecodeError as e:
            print("Keepvariable error, json loading failed - check whether json data is not corrupt: "+str(e))
            self.storage={}

    def lock(self, *args, **kwargs) -> RedisLock:
        """Create a fake lock, which does nothing but allows KeepVariableDummyRedisServer to conform to the interface."""
        class DummyLock:
            def acquire(self):
                return True

            def release(self):
                pass

        return DummyLock()

    def pipeline(self, *args, **kwargs) -> RedisPipeline:
        raise NotImplementedError("Pipelining operations is not available for DummyRedisServer")

    def set(self, key: str, value: Any, additional_params: Optional[dict] = None,
            **kwargs) -> dict[str, str]:
        additional_params = {} if additional_params is None else additional_params

        value = self.parse_saved_value(value, additional_params)
        self.storage[key] = value

        with open("kv_storage.json", "w") as file:
            final_json = "{" + ", ".join(
                f'"{key}": {value}' for key, value in self.storage.items()
            ) + "}"
            file.write(final_json)

        return {key: value}

    def get(self, key: str) -> Union[dict, pd.DataFrame, np.ndarray, datetime.datetime]:
        value = self.storage.get(key)

        # Do not move this condition to decode_loaded_value(), it only deals with missing keys
        if value is None:
            return None

        decoded_value = self.decode_loaded_value(value)
        return decoded_value

    def json_mset(self, name: str, params: dict, *args, **kwargs) -> None:
        """Set multiple keys in a JSON document.

        :param name: key under which a JSON document is stored
        :type name: str
        :param params: collection of arguments where keys are JSON Paths and values are values to set
        Keys are Redis Path strings, allowing access to specific elements withing JSON document
        More info: https://redis.io/docs/stack/json/path/
        :type params: dict

        e.g.
        params = {"$.is_saved"=true, "$.status"=SomeEnum.COMPLETED.value}
        """
        json_obj = self.decode_loaded_value(self.storage[name]) if name in self.storage else {}

        for json_path, value in params.items():
            element, final_key = access_element_by_path(json_obj, json_path)
            if element is None:
                json_obj = value
            elif final_key is None:
                element = value
            else:
                element[final_key] = value

        self.set(name, json_obj)

    def query(
        self,
        *,
        text_params: Optional[dict[str, tuple]] = None,
        tag_params: Optional[dict[str, tuple]] = None,
        entity_key: str,
        index_name: str = "index",
        field_to_sort_by: Optional[str] = None,
        asc=True,
        paginate: Optional[tuple[int, int]] = None,
        ignored_keywords: list[str] = None,
        **kwargs,
    ) -> dict[str, dict]:
        """Simplified alternative to RedisSearch. Allows to search and sort by values of specified fields.

        :param text_params: key name to a tuple of values mapping, e.g. {'status': ('pipel', ...), ...}
        :type text_params: dict[str, tuple]
        :param tag_params: key name to a tuple of values mapping, e.g. {'status': ('QUEUED', ...), ...}
        :type tag_params: dict[str, tuple]
        :param field_to_sort_by: attribute name by which results should be sorted
        :type field_to_sort_by: str
        :param asc: True if sort in ascending order, defaults to True
        :type asc: bool, optional
        :param paginate: pagination params in for of a tuple - (offset, limit), defaults to None
        :type paginate: Optional[tuple[int, int]], optional
        :return: [('jobs:43', job_dict), ...]
        :rtype: list[tuple]
        """
        def occurence_of_ignored_keywords(record_name: str, ignored_keywords: list[str]) -> bool:
            """Check validity of filtered records - omit ignored_keywords: pk, lock, ..."""
            are_ignored_keywords_occuring = any(x in record_name for x in ignored_keywords)
            return (are_ignored_keywords_occuring)

        if ignored_keywords is None:
            ignored_keywords = ["index", "pk", "lock"]

        found_records: list[tuple[str, dict]] = [
            (record_name, self.decode_loaded_value(value))
            for record_name, value in self.storage.items() if entity_key in record_name and
            not occurence_of_ignored_keywords(record_name, ignored_keywords)
        ]  # e.g. [('jobs:43', job_dict), ...]

        # TAG search
        if tag_params is not None:
            for field, values in tag_params.items():
                found_records = [
                    (record_id, record) for record_id, record in found_records
                    if record.get(field) and record.get(field) in values
                ]

        # TEXT search
        if text_params is not None:
            for field, values in text_params.items():
                for value in values:
                    # E.g. value = "QUEU", job.get(field) = "QUEUED"
                    found_records = {
                        (record_id, record) for record_id, record in found_records
                        if record.get(field) and value in record.get(field)
                    }

        if field_to_sort_by:
            found_records = sorted(found_records, key=lambda x: x[1][field_to_sort_by])
            if not asc:
                found_records.reverse()
        if paginate:
            start = paginate[0]
            end = paginate[0] + paginate[1]
            found_records = found_records[start:end]

        return dict(found_records)

    def arrlen(self, name: str, path: str, **kwargs) -> Optional[int]:
        try:
            json_obj = self.decode_loaded_value(self.storage[name]) if name in self.storage else {}

            element, final_key = access_element_by_path(json_obj, path)
            if element is None:
                return len(json_obj)
            elif final_key is None:
                return len(element)
            else:
                return len(element[final_key])
        except (KeyError, IndexError) as e:
            raise AssertionError(
                "Nested object does not exist - most probably due to incorrect path arg"
            ) from e

    def arrappend(self, name: str, path: str, objects: Iterable, **kwargs) -> Optional[int]:
        try:
            json_obj = self.decode_loaded_value(self.storage[name]) if name in self.storage else {}

            element, final_key = access_element_by_path(json_obj, path)
            if element is None:
                json_obj.extend(objects)
                array_length = len(json_obj)
            elif final_key is None:
                element.extend(objects)
                array_length = len(element)
            else:
                element[final_key].extend(objects)
                array_length = len(element[final_key])
        except (KeyError, IndexError) as e:
            raise AssertionError(
                "Nested object does not exist - most probably due to incorrect path arg"
            ) from e

        self.set(name, json_obj)
        return array_length

    def scan(self, match_string: str, *args, **kwargs) -> list[str]:
        """Find saved keys, matching their name with a given glob-style pattern. This command does not block the server, as it is based on a cursor-style iterator.

        :param match_string: string pattern to match keys against, e.g. 'jobs:*'
        :type match_string: str
        :return: list of found key names
        :rtype: list[str]
        """
        # Convert glob-style pattern to regex
        match_pattern = match_string.replace("*", ".*").replace("?", ".")
        results = [key for key in self.storage.keys() if re.search(match_pattern, key)]
        return results

    def delete(self, *names: str, **kwargs) -> int:
        return sum(1 for name in names if self.storage.pop(name, None))


class KeepVariableRedisServer(AbstractKeepVariableServer):
    def __init__(
        self, host: str = "localhost", port: int = 6379, db: int = 0, username: str = 'default',
        password: Optional[str] = None
    ):
        self.host: str = host
        self.port: int = port
        self.db = db
        self.username: str = username
        self.password: Optional[str] = password

        # Redis instance maintains connection pool internally, additionally it is thread-safe.
        self.redis = redis.Redis(
            host=self.host, port=self.port, username=self.username, db=self.db,
            password=self.password, decode_responses=True, charset="utf-8"
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

    def pipeline(self, *, transaction: bool = True) -> RedisPipeline:
        """Create a Redis Pipeline object, which can be used to execute multiple commands atomically."""
        return self.redis.pipeline(transaction=transaction)

    def set(
        self, key: str, value: str, additional_params: Optional[dict] = None, *,
        pipeline: Optional[RedisPipeline] = None
    ):
        if additional_params is None:
            additional_params = {}

        value = self.parse_saved_value(value, additional_params)

        if pipeline:
            return pipeline.set(key, value)
        else:
            return self.redis.set(key, value)

    def get(self, key: str) -> Optional[Any]:
        try:
            value = self.redis.get(key)

            # Do not move this condition to decode_loaded_value(), it only deals with missing keys
            if value is None:
                return value
            decoded_value = self.decode_loaded_value(value)
        # Raised when trying to get JSON document in Redis. JSON documents have their own get method
        except redis.exceptions.ResponseError:
            decoded_value = self.redis.json().get(key)
        return decoded_value

    def json_mset(
        self, name: str, params: dict[str, Any], *, pipeline: Optional[RedisPipeline] = None
    ) -> Optional[RedisPipeline]:
        """Set multiple keys in Redis JSON document. This method uses RedisJSON.

        :param name: Key of the Redis variable
        :type name: str
        :param params: dict where keys are JSON Paths and values are values to set
        Keys are Redis Path strings, allowing access to specific elements withing JSON document
        More info: https://redis.io/docs/stack/json/path/
        :type params: dict
        :param pipeline: pipeline in which operations can be executed in, defaults to None
        :type pipeline: Optional[RedisPipeline], optional
        :return: return pipeline if passed, otherwise only execute json set operation
        :rtype: Optional[RedisPipeline]

        e.g.
        params = {"$.is_saved"=true, "$.status"=SomeEnum.COMPLETED.value}
        """
        if pipeline:
            for json_xpath, value in params.items():
                pipeline.json().set(name, json_xpath, value)
            return pipeline

        with self.redis.pipeline() as pipe:
            for json_xpath, value in params.items():
                pipe.json().set(name, json_xpath, value)
            pipe.execute()

    def query(
        self, *, text_params: Optional[dict[str, tuple]] = None,
        tag_params: Optional[dict[str, tuple]] = None, entity_key: str, index_name: str = "index",
        field_to_sort_by: Optional[str] = None, asc=True,
        paginate: Optional[tuple[int, int]] = None, **kwargs
    ) -> dict:
        """Simplified wrapper to RedisSearch. Allows to search and sort by a value of Redis TAG/TEXT fields.

        Simplistic on purpose, to avoid bloat. Additional functionality should be added in case of need.
        :param text_params: key name to a tuple of values mapping, e.g. {'status': ('pipel', ...), ...}
        :type text_params: dict[str, tuple]
        :param tag_params: key name to a tuple of values mapping, e.g. {'status': ('QUEUED', ...), ...}
        :type tag_params: dict[str, tuple]
        :param entity_key: name of the Redis key that is preceding the index name - e.g. "jobs"
        :type entity_key: str
        :param index_name: name of the Redis index used for querying
        :type index_name: str
        :param field_to_sort_by: attribute name by which results should be sorted, defaults to None
        :type field_to_sort_by: Optional[str], optional
        :param asc: True if sort in ascending order, defaults to True
        :type asc: bool, optional
        :param paginate: pagination params in for of a tuple - (offset, limit), defaults to None
        :type paginate: Optional[tuple[int, int]], optional
        :return: {'jobs:43': job_dict, ...}
        :rtype: dict[str, Any]
        """
        final_query = ""

        if text_params is not None:
            text_query_template = "@{field}:{value}"
            for field, values in text_params.items():
                value_str = "|".join(values)
                final_query += text_query_template.format(field=field, value=value_str)

        if tag_params is not None:
            tag_query_template = "@{field}:{{{value}}}"
            for field, values in tag_params.items():
                value_str = "|".join(values)
                final_query += tag_query_template.format(field=field, value=value_str)

        # If no query was specified, return all records from the index
        if final_query == "":
            final_query = "*"

        # Query example: "@type:PIPEL @status:{QUEUED|COMPLETED}"
        # Explanation: find all jobs with type field containing 'PIPEL' and status being either 'QUEUED' or 'COMPLETED'
        query_object = Query(final_query)

        if field_to_sort_by:
            query_object.sort_by(field_to_sort_by, asc=asc)
        if paginate:
            query_object.paging(*paginate)

        assert len(entity_key) > 0  #entity needs to be specified
        index_key = entity_key + ":" + index_name
        job_docs: list = self.redis.ft(index_key).search(query_object).docs
        return {job_doc.id: self.decode_loaded_value(job_doc.json) for job_doc in job_docs}

    def arrlen(self, name: str, path: str, *,
               pipeline: Optional[RedisPipeline] = None) -> Union[int, None, RedisPipeline]:
        if pipeline:
            return pipeline.json().arrlen(name, path)
        return self.redis.json().arrlen(name, path).pop()

    def arrappend(
        self, name: str, path: str, objects: Iterable, *, pipeline: Optional[RedisPipeline] = None
    ) -> Union[int, None, RedisPipeline]:
        if pipeline:
            return pipeline.json().arrappend(name, path, *objects)
        return self.redis.json().arrappend(name, path, *objects).pop()

    def scan(self, match_string: str, count: int = 50, type_: Optional[str] = None) -> list[str]:
        """Find saved keys, matching their name with a given glob-style pattern.This command does not block the server, as it is based on a cursor-style iterator.

        https://redis.io/commands/scan/

        :param match_string: string pattern to match keys against, e.g. 'jobs:*'
        :type match_string: str
        :param count: how many rows, defaults to 50
        :type count: int, optional
        :param type_: filter on specified Redis key type, defaults to None
        :type type_: Optional[str], optional
        :return: list of found keys
        :rtype: list[str]
        """
        return list(self.redis.scan_iter(match_string, count, type_))

    def delete(self, *names: str,
               pipeline: Optional[RedisPipeline] = None) -> Union[int, RedisPipeline]:
        """Delete specified keys. If pipeline is passed, delete is executed in a transaction.

        :param pipeline: if provided, delete operation will be added to the existing pipeline
        :type pipeline: Optional[RedisPipeline]
        :return: number of deleted keys or a pipeline in case it was passed to a function
        :rtype: int | RedisPipeline
        """
        if pipeline:
            return pipeline.delete(*names)
        return self.redis.delete(*names)
