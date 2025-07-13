import sys
import types
from dataclasses import dataclass
from typing import Any, Dict, List, Union, cast, get_args, get_origin

import pyarrow as pa

if sys.version_info >= (3, 11):
    from typing import dataclass_transform
else:
    from typing_extensions import dataclass_transform


@dataclass_transform()
class DataMeta(type):
    """
    Metaclass for Data class to handle type annotations and schema generation.
    """

    def __new__(cls, name, bases, attrs):
        if not bases:
            return super().__new__(cls, name, bases, attrs)

        # --- Arrow Schema Generation ---
        base_schema = getattr(bases[0], "_schema", None)
        new_schema = generate_arrow_schema(attrs.get("__annotations__", {}))

        if base_schema is not None:
            if not isinstance(base_schema, pa.Schema):
                raise TypeError(
                    f"Base class {bases[0].__name__} must have a valid PyArrow schema."
                )
            new_schema = pa.schema(
                pa.struct(base_schema).fields + pa.struct(new_schema).fields
            )

        attrs["_schema"] = new_schema

        # --- Enforce super().__post_init__() chaining ---
        base_class_post_init = getattr(bases[0], "__post_init__", None)
        new_class_post_init = attrs.get("__post_init__", None)

        if base_class_post_init or new_class_post_init:

            def chained_post_init(self, *args, **kwargs):
                if base_class_post_init:
                    base_class_post_init(self)
                if new_class_post_init:
                    new_class_post_init(self, *args, **kwargs)

            # Set the new __post_init__ method
            attrs["__post_init__"] = chained_post_init

        new_cls = super().__new__(
            cls,
            name,
            bases,
            attrs,
        )

        # --- Handle Data Class Conversion ---
        return dataclass(
            cast(
                type,
                new_cls,
            )
        )

    def __repr__(cls):
        return f"{cls.__name__}({', '.join(f'{k}: {v.__name__}' for k, v in cls.__annotations__.items())})"


SUPPORTED_TYPES = [int, float, str, bool, pa.DataType]


def _generate_arrow_field_from_primitive_annotation(name, annotation: type):
    if hasattr(annotation, "_schema"):
        schema = getattr(annotation, "_schema", None)
        if schema is None:
            raise TypeError(f"Data type {annotation.__name__} has no schema defined.")
        return pa.field(name, pa.struct(schema))
    elif annotation is int:
        return pa.field(name, pa.int64())
    elif annotation is float:
        return pa.field(name, pa.float64())
    elif annotation is str:
        return pa.field(name, pa.string())
    elif annotation is bool:
        return pa.field(name, pa.bool_())
    elif annotation is timestamp:
        return pa.field(name, pa.timestamp("ns"))
    elif issubclass(annotation, pa.DataType):
        return pa.field(name, annotation)
    else:
        raise TypeError(f"Unsupported type: {annotation}")


def generate_arrow_schema(attrs):
    """
    Generate a PyArrow schema based on the type annotations of the class.
    Data types can be either primitive types, nested Data, or lists/tuples of these types.
    """
    fields = []
    for name, annotation in attrs.items():
        if get_origin(annotation) is list:
            item_type = get_args(annotation)[0]
            fields.append(
                pa.field(
                    name,
                    pa.list_(
                        _generate_arrow_field_from_primitive_annotation(
                            "item", item_type
                        )
                    ),
                )
            )
        elif get_origin(annotation) is tuple:
            item_types = get_args(annotation)
            fields.append(
                pa.field(
                    name,
                    pa.struct(
                        [
                            _generate_arrow_field_from_primitive_annotation(
                                f"item_{i}", t
                            )
                            for i, t in enumerate(item_types)
                        ]
                    ),
                )
            )
        elif (type(annotation) is types.UnionType) or (get_origin(annotation) is Union):
            union_types = get_args(annotation)
            non_none_types = [
                t
                for t in union_types
                if (isinstance(t, tuple(SUPPORTED_TYPES)) or hasattr(t, "_schema"))
            ]
            if len(non_none_types) == 1:
                fields.append(
                    _generate_arrow_field_from_primitive_annotation(
                        name, non_none_types[0]
                    )
                )
            else:
                raise TypeError(f"Unsupported Union type: {annotation}")
        elif isinstance(annotation, type):
            fields.append(
                _generate_arrow_field_from_primitive_annotation(name, annotation)
            )
        else:
            raise TypeError(f"Unsupported type: {annotation}")

    return pa.schema(fields)


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
