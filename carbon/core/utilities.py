from typing import Any, Dict, List, Sequence, Tuple, TypeVar, Union

# def is_equal_with_singleton(
#     a: object | Sequence[object], b: object | Sequence[object]
# ) -> bool:
#     """
#     Check equality between two objects or sequences.
#     Normalizes to lists to handle tuple/list comparison.
#     """

#     def is_sequence(obj):
#         return isinstance(obj, (list, tuple))

#     a_norm = list(a) if is_sequence(a) else [a]  # type: ignore
#     b_norm = list(b) if is_sequence(b) else [b]  # type: ignore

#     return a_norm == b_norm


T = TypeVar("T")


def ensure_tuple_format(data: Union[T, Sequence[T]]) -> Union[Tuple[T], Tuple[T, ...]]:
    """
    Normalize a single object or a sequence to a tuple.
    If the input is a single object, it returns a tuple with that object.
    If the input is already a sequence, it returns it as a tuple.
    """
    if isinstance(data, Sequence):
        return tuple(data)
    return (data,)


def flatten_single_row_arrow_dict(data_dict: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Flatten a single row of an Arrow-compatible dictionary.
    """
    flattened_dict = {}
    for key, value in data_dict.items():
        if isinstance(value, list) and len(value) == 1:
            flattened_dict[key] = value[0]
        else:
            flattened_dict[key] = value
    return flattened_dict
