import re
from typing import Optional, Union


class IncorrectPathError(Exception): ...


def access_element_by_path(json_obj: Union[dict, list], json_path: str) -> tuple: #[Optional[object], Optional[Union[str, int]]] #not compatible with Python 3.9
    """Traverse a JSON document under 'name' to access the object defined by the 'path' argument.

    :param obj: reference to the traversed object
    :type obj: list | dict
    :param path: Redis JSON path string e.g. "$.job.nodes[2].status"
    :type path: str
    :return: tuple[referenced object, key]
    :rtype: tuple[Optional[object], Optional[Union[str, int]]]

    As Python does not have pointers, we have to trick it by passing a reference.
    We're returning a reference to the object 1 level above the final one dictated by the 'path' argument
    Additionally, we are returning the key (or index) to the last value we want to access
    We can use these return values to access object described by 'path' in the original function

    tuple[referenced_object, key] --> referenced_object[key] = ...

    If referenced object is None, overwrite the json_obj itself, as the reference cannot be
    constructed for outermost object.
    """
    path_stack = parse_path_to_stack(json_path)
    current_obj = json_obj

    if not path_stack: # With the empty stack, no reference can be passed - object must be overwritten directly
        return None, None

    try:
        for key in path_stack[:-1]:
            current_obj = current_obj[key]
        final_key = path_stack[-1]
    except (AttributeError, IndexError) as e:
        raise IncorrectPathError(f"Path '{json_path}' could not be accessed") from e

    return current_obj, final_key  # Returning parent object and final key or index

def parse_path_to_stack(json_path: str) -> list: #[Union[int, str]] #not compatible with python 3.9
    """Deconstruct path string into a stack of references allowing traversal.

    :param json_path: Redis JSON path string e.g. "$.job.nodes[2].status"
    :type json_path: str
    :return: ["$", "job", "nodes", 2, "status"]
    :rtype: list[Union[int, str]]

    e.g. "$.job.nodes[2].status" -> ["$", "job", "nodes", 2, "status"]
    """
    elements = json_path.split(".")
    pattern = r"\[(\d+)\]"  # Find any number consisting of 1 or more digits, enclosed by []
    stack = []

    try:
        for element in elements:
            key = element.split("[")[0]
            if key != "$": # Omit root element
                stack.append(key)

            matches = re.findall(pattern, element)
            for match in matches:
                index = int(match)
                stack.append(index)
    except (AttributeError, IndexError, TypeError) as e:
        raise IncorrectPathError(f"Path '{json_path}' could not be accessed") from e

    return stack
