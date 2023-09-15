import re
from typing import Any, Optional, Union

def access_element_by_path(json_obj: dict, json_path: str) -> tuple[Optional[object], Optional[Union[str, int]]]:
    path_stack = parse_path_to_stack(json_path)
    current_obj = json_obj

    if not path_stack:
        return None, None

    for key in path_stack[:-1]:
        current_obj = current_obj[key]

    final_key = path_stack[-1] if path_stack else None
    return current_obj, final_key  # Returning parent object and final key or index

def parse_path_to_stack(json_path: str) -> list[Union[int, str]]:
    elements = json_path.split(".")
    pattern = r"\[(\d+)\]"  # Find any number consisting of 1 or more digits, enclosed by []
    stack = []
    for elem in elements[1:]:
        key = elem.split("[")[0]
        stack.append(key)

        matches = re.findall(pattern, elem)
        for match in matches:
            index = int(match)
            stack.append(index)

    return stack

# def access_element_by_path(json_obj: dict, json_path: str):
#     stack: list = parse_path_to_stack(json_path)

    # curr_obj = json_obj
    # while len(stack) > 1:
    #     key = stack.pop(0)
    #     if stack and isinstance(stack[0], int):
    #         index = stack.pop(0)
    #         curr_obj = curr_obj.setdefault(key, [])[index]
    #     elif isinstance(stack[0], str):
    #         curr_obj = curr_obj.setdefault(key, {})

    # final_key = stack.pop(0)
    # return curr_obj, final_key  # Returning parent object and final key or index

# def parse_path_to_stack(json_path: str):
#     elements = json_path.split(".")
#     pattern = r"\[(\d+)\]"  # Find any "[*]" group where * is one or more digits
#     stack = []
#     for elem in elements:
#         match = re.find(pattern, elem)
#         if match:
#             key, index_str = elem.split("[")
#             index = int(index_str[:-1])
#             stack.extend([key, index])
#         else:
#             stack.append(elem)
#     return stack

def _extract_object_from_path(self, obj: Union[list, dict], name: str, path: str) -> tuple[Any, str, Optional[int]]:
    """Recursively traverses a JSON document under 'name' to access the object defined by the 'path' argument.

    :param obj: key under which a JSON document is stored
    :type obj: list | dict
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
    nested_object = obj
    for element, index in zip(element_list[:-1], index_list[:-1]):
        if index is not None:
            nested_object = nested_object[element][index]
        else:
            nested_object = nested_object[element]

    return nested_object, element_list[-1], index_list[-1]
