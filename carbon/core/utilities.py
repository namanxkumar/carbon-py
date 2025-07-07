from typing import Sequence, Tuple, TypeVar, Union

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
