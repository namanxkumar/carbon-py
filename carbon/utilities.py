from typing import Sequence


def is_equal_with_singleton(
    a: object | Sequence[object], b: object | Sequence[object]
) -> bool:
    """
    Check equality between two objects or sequences.
    Normalizes to lists to handle tuple/list comparison.
    """

    def is_sequence(obj):
        return isinstance(obj, (list, tuple))

    a_norm = list(a) if is_sequence(a) else [a]  # type: ignore
    b_norm = list(b) if is_sequence(b) else [b]  # type: ignore

    return a_norm == b_norm
