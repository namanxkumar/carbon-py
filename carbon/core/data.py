import sys
import types
from dataclasses import Field, dataclass
from typing import Dict, List, Union, cast

import pyarrow as pa

if sys.version_info >= (3, 11):
    from typing import dataclass_transform
else:
    from typing_extensions import dataclass_transform


class Autofill:
    """
    Marker class for fields that should be automatically filled in by the framework.
    This is used to indicate that a field should be populated with a default value
    or by the framework during data processing.
    """

    pass


@dataclass_transform()
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

        return dataclass(
            cast(
                type,
                super().__new__(
                    cls,
                    name,
                    bases,
                    attrs,
                ),
            )
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
        elif (type(annotation) is types.UnionType) or (
            hasattr(annotation, "__origin__") and annotation.__origin__ is Union
        ):
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

    def _to_arrow_compatible_dict(self):
        """
        Convert data into a dictionary recursively.
        """
        result = {}
        for key, value in self.__dict__.items():
            if isinstance(value, Data):
                result[key] = value._to_arrow_compatible_dict()
            elif isinstance(value, list):
                result[key] = [
                    v._to_arrow_compatible_dict() if isinstance(v, Data) else v
                    for v in value
                ]
            elif isinstance(value, tuple):
                result[key] = {
                    f"item_{i}": v._to_arrow_compatible_dict()
                    if isinstance(v, Data)
                    else v
                    for i, v in enumerate(value)
                }
            else:
                result[key] = value
        return result

    def to_arrow_record_batch(self):
        """
        Convert the data to a PyArrow RecordBatch.
        """

        return pa.RecordBatch.from_pylist(
            [self._to_arrow_compatible_dict()], schema=self.schema
        )

    def to_arrow_table(self):
        """
        Convert the data to a PyArrow Table.
        """
        return pa.Table.from_pylist(
            [self._to_arrow_compatible_dict()], schema=self.schema
        )

    @classmethod
    def from_arrow_compatible_dict(cls, data_dict: Dict) -> "Data":
        """
        Create an instance of the Data class from a dictionary that is compatible with PyArrow.
        This method assumes that the dictionary has the same schema as the Data class.
        """
        if not isinstance(data_dict, dict):
            raise TypeError("Expected a dictionary.")

        assert hasattr(cls, "__dataclass_fields__"), (
            "Data must be a dataclass with type annotations."
        )

        fields_dict: Dict[str, Field] = getattr(cls, "__dataclass_fields__", {})

        # Get field names and types for the current class
        for field_name, field_ in fields_dict.items():
            if field_name not in data_dict:
                raise ValueError(f"Missing field '{field_name}' in data dictionary.")

            value = data_dict[field_name]
            field_type = cast(type, field_.type)

            if hasattr(field_type, "__origin__") and field_type.__origin__ is list:
                # Handle lists
                item_type = field_type.__args__[0]
                if not isinstance(value, list):
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a list of {item_type}, but got {type(value)}."
                    )
                data_dict[field_name] = [
                    item_type.from_arrow_compatible_dict(v)
                    if isinstance(v, dict) and issubclass(item_type, Data)
                    else v
                    for v in value
                ]
            elif hasattr(field_type, "__origin__") and field_type.__origin__ is tuple:
                # Handle tuples
                item_types = field_type.__args__
                if not isinstance(value, dict):
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a dict for tuple items, but got {type(value)}."
                    )
                data_dict[field_name] = tuple(
                    item_type.from_arrow_compatible_dict(value[f"item_{i}"])
                    if isinstance(value.get(f"item_{i}"), dict)
                    and issubclass(item_type, Data)
                    else value.get(f"item_{i}")
                    for i, item_type in enumerate(item_types)
                )
            elif isinstance(field_type, types.UnionType) or (
                hasattr(field_type, "__origin__") and field_type.__origin__ is Union
            ):
                # Handle Union types
                union_types = field_type.__args__
                non_none_types = [t for t in union_types if t is not Autofill]
                if len(non_none_types) == 1:
                    item_type = non_none_types[0]
                    if isinstance(value, dict) and issubclass(item_type, Data):
                        data_dict[field_name] = item_type.from_arrow_compatible_dict(
                            value
                        )
                    else:
                        data_dict[field_name] = value
                else:
                    raise TypeError(f"Unsupported Union type: {field_type}")
            elif isinstance(field_type, type) and issubclass(field_type, Data):
                # Handle Data types
                if isinstance(value, dict):
                    data_dict[field_name] = field_type.from_arrow_compatible_dict(value)
                else:
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a dict for Data type, but got {type(value)}."
                    )
            elif isinstance(value, field_type):
                # Handle primitive types
                data_dict[field_name] = value
            else:
                raise TypeError(
                    f"Field '{field_name}' is expected to be of type {field_type}, but got {type(value)}."
                )

        # Create an instance of the Data class with the populated fields
        return cls(**data_dict)

    @classmethod
    def from_arrow_record_batch(cls, record_batch: pa.RecordBatch) -> "Data":
        """
        Create an instance of the Data class from a PyArrow RecordBatch.
        This method assumes that the RecordBatch has the same schema as the Data class.
        """
        if not isinstance(record_batch, pa.RecordBatch):
            raise TypeError("Expected a PyArrow RecordBatch.")

        assert hasattr(cls, "_schema"), (
            "Data class must have a schema defined. "
            "Ensure you are using the Data base class on a dataclass with type annotations."
        )

        if record_batch.schema != getattr(cls, "_schema"):
            raise ValueError("RecordBatch schema does not match the Data class schema.")

        if record_batch.num_rows != 1:
            raise ValueError(
                "RecordBatch must contain exactly one row for Data class instantiation."
            )

        data_dict = record_batch.to_pydict()

        data_dict = {k: v[0] for k, v in data_dict.items()}  # Flatten to single row

        return cls.from_arrow_compatible_dict(data_dict)

    @classmethod
    def from_arrow_table(cls, table: pa.Table) -> "Data":
        """
        Create an instance of the Data class from a PyArrow Table.
        This method assumes that the Table has the same schema as the Data class.
        """
        if not isinstance(table, pa.Table):
            raise TypeError("Expected a PyArrow Table.")

        assert hasattr(cls, "_schema"), (
            "Data class must have a schema defined. "
            "Ensure you are using the Data base class on a dataclass with type annotations."
        )

        if table.schema != getattr(cls, "_schema"):
            raise ValueError("Table schema does not match the Data class schema.")

        if table.num_rows != 1:
            raise ValueError(
                "Table must contain exactly one row for Data class instantiation."
            )

        data_dict = table.to_pydict()

        data_dict = {k: v[0] for k, v in data_dict.items()}  # Flatten to single row

        return cls.from_arrow_compatible_dict(data_dict)

    def _get_autofill_fields(self, field_type: type):
        """
        Autofill fields that are marked with Autofill.
        This method can be overridden in subclasses to provide custom autofill logic.
        """
        if field_type is Header:
            # Example autofill logic for Header
            return Header(seq=0, stamp=0.0, frame_id="default_frame")
        else:
            raise NotImplementedError(
                f"Autofill logic not implemented for field type: {field_type.__name__}"
            )

    def __post_init__(self):
        """
        Post-initialization hook for Data classes.
        This can still be overridden in subclasses to add custom initialization logic, however
        this implementation will always run first to ensure the base class is initialized.
        """
        # Replace fields with Autofill if they are not set
        for field_name, field_value in self.__dict__.items():
            if isinstance(field_value, Autofill):
                # Replace with a default value or leave as Autofill
                # Get field type from annotations
                field_type = cast(
                    type,
                    cast(
                        Field,
                        cast(Dict, getattr(self.__class__, "__dataclass_fields__")).get(
                            field_name
                        ),
                    ).type,
                )
                assert isinstance(field_type, types.UnionType) or (
                    hasattr(field_type, "__origin__") and field_type.__origin__ is Union
                ), (
                    f"Field '{field_name}' must be a Union type with Autofill and the type to be autofilled."
                )

                # Handle Union types
                union_types = field_type.__args__
                non_none_types = [t for t in union_types if t is not Autofill]

                if len(non_none_types) == 1:
                    field_type = non_none_types[0]
                else:
                    raise TypeError(f"Unsupported Union type: {field_type}")

                self.__dict__[field_name] = self._get_autofill_fields(
                    field_type=field_type
                )


class Header(Data):
    seq: int
    stamp: float
    frame_id: str


class StampedData(Data):
    header: Union[Header, Autofill]


if __name__ == "__main__":
    # Example usage
    class NestedData(Data):
        value: int
        description: str

    class CustomData(StampedData):
        nested: NestedData
        name: str
        value: List[int]

    data_instance = CustomData(
        header=Autofill(),
        nested=NestedData(value=42, description="Example nested data"),
        name="example",
        value=[42],
    )

    record_batch = data_instance.to_arrow_record_batch()

    table = data_instance.to_arrow_table()

    # Convert back from RecordBatch
    new_instance = CustomData.from_arrow_record_batch(record_batch)
    print(new_instance)

    # Convert back from Table
    new_instance_from_table = CustomData.from_arrow_table(table)
    print(new_instance_from_table)
