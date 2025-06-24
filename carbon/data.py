from dataclasses import dataclass

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

        return super().__new__(
            cls,
            name,
            bases,
            dict(
                attrs, _schema=_generate_arrow_schema(attrs.get("__annotations__", {}))
            ),
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


if __name__ == "__main__":
    # Example usage
    @dataclass
    class CustomData(Data):
        name: str
        value: tuple[int]

    data_instance = CustomData(name="example", value=(42,))
    print(
        data_instance
    )  # Output: name: string, value: struct<sub_name: string, sub_value: int64>
    # print(CustomData.schema())  # Output: example
