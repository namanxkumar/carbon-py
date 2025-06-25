from dataclasses import dataclass
from typing import Union

import pyarrow as pa


class DataMeta(type):
    """
    Metaclass for Data class to handle type annotations and schema generation.
    """

    def __new__(cls, name, bases, attrs):
        if not bases:
            # If no bases, create a new type
            return super().__new__(cls, name, bases, attrs)

        assert issubclass(bases[0], Data), (
            "Data classes must inherit from the Data base class."
        )

        # Get the schema of the base class if it exists
        base_schema = getattr(bases[0], "_schema", None)
        new_schema = _generate_arrow_schema(attrs.get("__annotations__", {}))

        # Merge the base schema with the new schema
        if base_schema is not None:
            # Ensure the base schema is a PyArrow schema
            if not isinstance(base_schema, pa.Schema):
                raise TypeError(
                    f"Base class {bases[0].__name__} must have a valid PyArrow schema."
                )
            new_schema = pa.schema(
                pa.struct(base_schema).fields + pa.struct(new_schema).fields
            )

        return super().__new__(
            cls,
            name,
            bases,
            dict(attrs, _schema=new_schema),
        )

    def __repr__(cls):
        return f"{cls.__name__}({', '.join(f'{k}: {v.__name__}' for k, v in cls.__annotations__.items())})"


def _generate_arrow_field_from_primitive_annotation(name, annotation: type):
    # Primitive type
    if issubclass(annotation, Data):
        schema = getattr(annotation, "_schema", None)
        if schema is None:
            raise TypeError(f"Data type {annotation.__name__} has no schema defined.")
        return pa.field(name, pa.struct(schema))
    elif issubclass(annotation, pa.DataType):
        return pa.field(name, annotation)
    elif annotation is int:
        return pa.field(name, pa.int64())
    elif annotation is float:
        return pa.field(name, pa.float64())
    elif annotation is str:
        return pa.field(name, pa.string())
    elif annotation is bool:
        return pa.field(name, pa.bool_())
    else:
        raise TypeError(f"Unsupported type: {annotation}")


def _generate_arrow_schema(attrs):
    """
    Generate a PyArrow schema based on the type annotations of the class.
    Data types can be either primitive types, nested Data, or lists/tuples of these types.
    """
    fields = []
    for name, annotation in attrs.items():
        if hasattr(annotation, "__origin__") and annotation.__origin__ is list:
            item_type = annotation.__args__[0]
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
        elif hasattr(annotation, "__origin__") and annotation.__origin__ is tuple:
            item_types = annotation.__args__
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
        elif hasattr(annotation, "__origin__") and annotation.__origin__ is Union:
            union_types = annotation.__args__
            non_none_types = [t for t in union_types if t is not Autofill]
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


class Data(metaclass=DataMeta):
    """
    Base class for data passed between modules in the Carbon framework.
    Subclass this and define fields using type annotations to create custom datatypes.
    """

    def __init__(self):
        pass

    @property
    def schema(self) -> pa.Schema:
        """
        Returns the PyArrow schema for this data type.
        This is generated based on the type annotations of the class.
        """
        assert hasattr(self, "_schema"), (
            "Data class must have a schema defined. "
            "Ensure you are using the Data base class on a dataclass with type annotations."
        )
        return getattr(self, "_schema")

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(f'{k}={v}' for k, v in self.__dict__.items())})"


@dataclass
class Header(Data):
    seq: int
    stamp: float
    frame_id: str


# Create an autofill marker type
class Autofill(Data):
    """
    A marker class for autofill data types.
    This can be used to indicate that a field should be filled automatically.
    """

    pass


@dataclass
class StampedData(Data):
    header: Union[Header, Autofill]


if __name__ == "__main__":
    # Example usage
    @dataclass
    class CustomData(StampedData):
        name: str
        value: tuple[int]

    data_instance = CustomData(
        header=Header(seq=1, stamp=1234567890.0, frame_id="base_frame"),
        name="example",
        value=(42,),
    )
    print(data_instance.schema)
