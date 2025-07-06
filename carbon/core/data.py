import types
from dataclasses import Field
from typing import Dict, List, Union, cast, get_args, get_origin

import pyarrow as pa

from carbon.core.data_utilities import Autofill, DataMeta
from carbon.core.utilities import flatten_single_row_arrow_dict


class Data(metaclass=DataMeta):
    """
    Base class for data passed between modules in the Carbon framework.
    Subclass this and define fields using type annotations to create custom datatypes.
    """

    @classmethod
    def get_schema(cls) -> pa.Schema:
        """
        Generate the PyArrow schema for this data type based on the type annotations.
        This method is called automatically by the DataMeta metaclass.
        """
        if not hasattr(cls, "_schema"):
            raise ValueError(
                "Data class must have a schema defined. "
                "Ensure you are using the Data base class with type annotations."
            )
        return getattr(cls, "_schema")

    @property
    def schema(self) -> pa.Schema:
        """
        Returns the PyArrow schema for this data type.
        This is generated based on the type annotations of the class.
        """
        assert hasattr(self, "_schema"), (
            "Data class must have a schema defined. "
            "Ensure you are using the Data base class with type annotations."
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

    def to_arrow_record_batch(self) -> pa.RecordBatch:
        """
        Convert the data to a PyArrow RecordBatch.
        """

        return pa.RecordBatch.from_pylist(
            [self._to_arrow_compatible_dict()], schema=self.schema
        )

    def to_arrow_table(self) -> pa.Table:
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
        fields_dict: Dict[str, Field] = getattr(cls, "__dataclass_fields__", {})

        arrow_dict = {}

        for field_name, field_ in fields_dict.items():
            if field_name not in data_dict:
                raise ValueError(f"Missing field '{field_name}' in data dictionary.")

            value = data_dict[field_name]
            field_type = cast(type, field_.type)

            if get_origin(field_type) is list:
                item_type = get_args(field_type)[0]
                if not isinstance(value, list):
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a list of {item_type}, but got {type(value)}."
                    )
                arrow_dict[field_name] = [
                    item_type.from_arrow_compatible_dict(v)
                    if isinstance(v, dict) and issubclass(item_type, Data)
                    else v
                    for v in value
                ]
            elif get_origin(field_type) is tuple:
                item_types = get_args(field_type)
                if not isinstance(value, dict):
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a dict for tuple items, but got {type(value)}."
                    )
                arrow_dict[field_name] = tuple(
                    item_type.from_arrow_compatible_dict(value[f"item_{i}"])
                    if isinstance(value.get(f"item_{i}"), dict)
                    and issubclass(item_type, Data)
                    else value.get(f"item_{i}")
                    for i, item_type in enumerate(item_types)
                )
            elif isinstance(field_type, types.UnionType) or (
                get_origin(field_type) is Union
            ):
                union_types = get_args(field_type)
                non_none_types = [t for t in union_types if t is not Autofill]
                if len(non_none_types) == 1:
                    item_type = non_none_types[0]
                    if isinstance(value, dict) and issubclass(item_type, Data):
                        arrow_dict[field_name] = item_type.from_arrow_compatible_dict(
                            value
                        )
                    else:
                        arrow_dict[field_name] = value
                else:
                    raise TypeError(f"Unsupported Union type: {field_type}")
            elif isinstance(field_type, type) and issubclass(field_type, Data):
                if isinstance(value, dict):
                    arrow_dict[field_name] = field_type.from_arrow_compatible_dict(
                        value
                    )
                else:
                    raise TypeError(
                        f"Field '{field_name}' is expected to be a dict for Data type, but got {type(value)}."
                    )
            elif isinstance(value, field_type):
                arrow_dict[field_name] = value
            else:
                raise TypeError(
                    f"Field '{field_name}' is expected to be of type {field_type}, but got {type(value)}."
                )

        return cls(**arrow_dict)

    @classmethod
    def from_arrow(cls, arrow_data: Union[pa.RecordBatch, pa.Table]) -> "Data":
        """
        Create an instance of the Data class from a PyArrow RecordBatch or Table.
        """
        if arrow_data.schema != cls.get_schema():
            raise ValueError(
                "Arrow Table or RecordBatch schema does not match the Data class schema."
            )

        if arrow_data.num_rows != 1:
            raise ValueError(
                "Arrow Table or RecordBatch must contain exactly one row for Data class instantiation."
            )

        return cls.from_arrow_compatible_dict(
            flatten_single_row_arrow_dict(arrow_data.to_pydict())
        )

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
        for field_name, field_value in self.__dict__.items():
            if not isinstance(field_value, Autofill):
                continue

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

            non_none_types = [t for t in field_type.__args__ if t is not Autofill]

            if len(non_none_types) == 1:
                field_type = non_none_types[0]
            else:
                raise TypeError(f"Unsupported Union type: {field_type}")

            self.__dict__[field_name] = self._get_autofill_fields(field_type=field_type)


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
    new_instance = CustomData.from_arrow(record_batch)
    print(new_instance)

    # Convert back from Table
    new_instance_from_table = CustomData.from_arrow(table)
    print(new_instance_from_table)
