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

        return super().__new__(
            cls,
            name,
            bases,
            dict(
                attrs, _schema=_generate_arrow_schema(attrs.get("__annotations__", {}))
            ),
        )


def _generate_arrow_field_from_primitive_annotation(name, annotation: type):
    # Primitive type
    if issubclass(annotation, Data):
        schema = annotation._schema
        if schema is None:
            raise TypeError(f"Data type {annotation.__name__} has no schema defined.")
        return pa.field(name, pa.struct(annotation._schema))
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

    def __init__():
        pass

    @property
    def schema(self) -> pa.Schema:
        """
        Returns the PyArrow schema for this data type.
        This is generated based on the type annotations of the class.
        """
        return self._schema


if __name__ == "__main__":
    # Example usage
    @dataclass
    class SubData(Data):
        sub_name: str
        sub_value: int

    # print(SubData.schema)  # Output: sub_name: string, sub_value: int64

    @dataclass
    class CustomData(Data):
        name: str
        value: tuple[SubData]

    data_instance = CustomData(name="example", value=[42])
    print(
        data_instance.schema
    )  # Output: name: string, value: struct<sub_name: string, sub_value: int64>
    # print(CustomData.schema())  # Output: example
