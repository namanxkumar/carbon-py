import inspect
import io
import json
from datetime import date, datetime
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Type, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator


class CarbonData(BaseModel):
    """
    Represents data passed between modules in the Carbon framework.
    Is able to serialize and deserialize data for communication over the Apache Arrow protocol.
    Now with Pydantic validation support.
    """

    # Class metadata - can be overridden in subclasses
    _carbon_type_name: ClassVar[str] = "CarbonData"
    _carbon_description: ClassVar[str] = "Base Carbon data type"
    _carbon_version: ClassVar[str] = "1.0"

    # Pydantic fields - these will be validated
    data: Optional[Any] = Field(
        None, description="The raw data to be converted to Arrow format"
    )
    schema: Optional[pa.Schema] = Field(
        None, description="Optional Arrow schema for validation"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict, description="Custom metadata"
    )

    # Internal Arrow table (not validated by Pydantic)
    _arrow_table: Optional[pa.Table] = None

    class Config:
        # Allow arbitrary types (for Arrow objects)
        arbitrary_types_allowed = True
        # Validate assignment after init
        validate_assignment = True
        # Custom JSON encoder for Arrow objects
        json_encoders = {
            pa.Schema: lambda v: str(v),
            pa.Table: lambda v: v.to_pydict(),
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
        }

    def __init__(self, **kwargs):
        """Initialize with automatic Arrow conversion and metadata injection."""
        super().__init__(**kwargs)

        # Convert data to Arrow if provided
        if self.data is not None:
            self._arrow_table = self._convert_to_arrow(self.data, self.schema)
            self._inject_type_metadata()

        # For subclasses that define fields directly, convert those to Arrow
        elif self._has_data_fields():
            field_data = self._extract_field_data()
            self._arrow_table = self._convert_to_arrow(field_data)
            self._inject_type_metadata()

    def _has_data_fields(self) -> bool:
        """Check if this subclass defines data fields beyond the base fields."""
        base_fields = {"data", "schema", "metadata"}
        return len(set(self.__fields__.keys()) - base_fields) > 0

    def _extract_field_data(self) -> Dict[str, Any]:
        """Extract data from Pydantic fields for Arrow conversion."""
        base_fields = {"data", "schema", "metadata"}
        field_data = {}

        for field_name, field_value in self:
            if field_name not in base_fields:
                field_data[field_name] = field_value

        return field_data

    def _inject_type_metadata(self):
        """Add type metadata to Arrow schema."""
        if self._arrow_table is not None:
            metadata = {
                "carbon_type": self._carbon_type_name,
                "carbon_version": self._carbon_version,
                "carbon_description": self._carbon_description,
                "created_at": datetime.now().isoformat(),
            }

            # Merge with existing metadata
            metadata.update(self.metadata)

            # Update schema with metadata
            current_schema = self._arrow_table.schema
            new_schema = current_schema.with_metadata(metadata)
            self._arrow_table = self._arrow_table.cast(new_schema)

    @model_validator(mode="before")
    def validate_data_compatibility(cls, values):
        """Validate that data is compatible with Arrow conversion."""
        data = values.get("data")
        schema = values.get("schema")

        if data is not None:
            try:
                # Try to convert to check compatibility
                temp_instance = object.__new__(cls)
                temp_instance._convert_to_arrow(data, schema)
            except Exception as e:
                raise ValueError(f"Data not compatible with Arrow conversion: {e}")

        return values

    @field_validator("schema")
    def validate_schema(cls, v):
        """Validate Arrow schema."""
        if v is not None and not isinstance(v, pa.Schema):
            raise ValueError("Schema must be a pyarrow.Schema object")
        return v

    def _convert_to_arrow(
        self, data: Any, schema: Optional[pa.Schema] = None
    ) -> pa.Table:
        """Convert various data types to Arrow Table with enhanced type handling."""

        if isinstance(data, pa.Table):
            return data
        elif isinstance(data, pa.RecordBatch):
            return pa.Table.from_batches([data])
        elif isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data, schema=schema)
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                return pa.table([data], names=["values"], schema=schema)
            elif data.ndim == 2:
                columns = [data[:, i] for i in range(data.shape[1])]
                names = [f"col_{i}" for i in range(data.shape[1])]
                return pa.table(columns, names=names, schema=schema)
            else:
                raise ValueError("Arrays with more than 2 dimensions not supported")
        elif isinstance(data, dict):
            return self._dict_to_arrow(data, schema)
        elif isinstance(data, list):
            if data and isinstance(data[0], dict):
                return pa.Table.from_pylist(data, schema=schema)
            else:
                return pa.table([data], names=["values"], schema=schema)
        else:
            return pa.table([[data]], names=["values"], schema=schema)

    def _dict_to_arrow(
        self, data: Dict, schema: Optional[pa.Schema] = None
    ) -> pa.Table:
        """Convert dictionary to Arrow table with schema validation."""
        flattened = {}

        for key, value in data.items():
            if isinstance(value, (list, np.ndarray)):
                flattened[key] = value
            elif isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    flattened[f"{key}.{nested_key}"] = [nested_value]
            elif isinstance(value, Enum):
                flattened[key] = [value.value]
            elif isinstance(value, (datetime, date)):
                flattened[key] = [value]
            else:
                flattened[key] = [value]

        return pa.table(flattened, schema=schema)

    @classmethod
    def from_arrow(cls, arrow_data: Union[pa.Table, pa.RecordBatch]) -> "CarbonData":
        """Create CarbonData from Arrow data."""
        instance = cls(data=None)
        if isinstance(arrow_data, pa.RecordBatch):
            instance._arrow_table = pa.Table.from_batches([arrow_data])
        else:
            instance._arrow_table = arrow_data

        # Extract metadata if it exists
        if arrow_data.schema.metadata:
            metadata = {
                k.decode(): v.decode() for k, v in arrow_data.schema.metadata.items()
            }
            instance.metadata.update(metadata)

        return instance

    @classmethod
    def from_json(cls, json_str: str) -> "CarbonData":
        """Create CarbonData from JSON string with validation."""
        try:
            data = json.loads(json_str)
            return cls(data=data)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON: {e}", cls)

    @classmethod
    def from_csv(cls, csv_path: str, **kwargs) -> "CarbonData":
        """Create CarbonData from CSV file."""
        try:
            df = pd.read_csv(csv_path, **kwargs)
            return cls(data=df)
        except Exception as e:
            raise ValidationError(f"Error reading CSV: {e}", cls)

    @classmethod
    def from_parquet(cls, parquet_path: str) -> "CarbonData":
        """Create CarbonData from Parquet file."""
        try:
            table = pa.parquet.read_table(parquet_path)
            return cls.from_arrow(table)
        except Exception as e:
            raise ValidationError(f"Error reading Parquet: {e}", cls)

    # Data conversion methods
    def to_pandas(self) -> pd.DataFrame:
        """Convert to pandas DataFrame."""
        if self._arrow_table is None:
            return pd.DataFrame()
        return self._arrow_table.to_pandas()

    def to_numpy(self) -> np.ndarray:
        """Convert to numpy array."""
        df = self.to_pandas()
        return df.values

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        if self._arrow_table is None:
            return {}
        return self._arrow_table.to_pydict()

    def to_list(self) -> List[Dict]:
        """Convert to list of dictionaries."""
        if self._arrow_table is None:
            return []
        return self._arrow_table.to_pylist()

    def to_json(self) -> str:
        """Convert to JSON string using Pydantic's JSON encoder."""
        return self.json()

    def serialize(self) -> bytes:
        """Serialize using Arrow IPC format."""
        if self._arrow_table is None:
            return b""

        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, self._arrow_table.schema) as writer:
            writer.write_table(self._arrow_table)
        return buffer.getvalue()

    @classmethod
    def deserialize(cls, data: bytes) -> "CarbonData":
        """Deserialize from bytes."""
        buffer = io.BytesIO(data)
        reader = pa.ipc.open_stream(buffer)
        table = reader.read_all()
        return cls.from_arrow(table)

    def validate_schema_compatibility(self, expected_schema: pa.Schema) -> bool:
        """Validate that current data matches expected schema."""
        if self._arrow_table is None:
            return False

        current_schema = self._arrow_table.schema

        # Check column names and types
        if len(current_schema) != len(expected_schema):
            return False

        for current_field, expected_field in zip(current_schema, expected_schema):
            if current_field.name != expected_field.name:
                return False
            if not current_field.type.equals(expected_field.type):
                return False

        return True

    def save(self, path: str, format: str = "parquet"):
        """Save data to file."""
        if self._arrow_table is None:
            raise ValueError("No data to save")

        if format.lower() == "parquet":
            pa.parquet.write_table(self._arrow_table, path)
        elif format.lower() == "csv":
            df = self.to_pandas()
            df.to_csv(path, index=False)
        elif format.lower() == "json":
            with open(path, "w") as f:
                f.write(self.to_json())
        else:
            raise ValueError(f"Unsupported format: {format}")

    # Data manipulation methods
    def filter(self, condition) -> "CarbonData":
        """Filter data based on condition."""
        if self._arrow_table is None:
            return self.__class__()

        if callable(condition):
            df = self.to_pandas()
            filtered_df = df[condition(df)]
            return self.__class__(data=filtered_df)
        else:
            filtered_table = self._arrow_table.filter(condition)
            return self.__class__.from_arrow(filtered_table)

    def select(self, columns: List[str]) -> "CarbonData":
        """Select specific columns."""
        if self._arrow_table is None:
            return self.__class__()

        selected_table = self._arrow_table.select(columns)
        return self.__class__.from_arrow(selected_table)

    def sort(self, by: Union[str, List[str]], ascending: bool = True) -> "CarbonData":
        """Sort data by column(s)."""
        if self._arrow_table is None:
            return self.__class__()

        if isinstance(by, str):
            by = [by]

        sort_keys = [(col, "ascending" if ascending else "descending") for col in by]
        sorted_table = self._arrow_table.sort_by(sort_keys)
        return self.__class__.from_arrow(sorted_table)

    @property
    def shape(self) -> tuple:
        """Get shape of the data."""
        if self._arrow_table is None:
            return (0, 0)
        return (self._arrow_table.num_rows, self._arrow_table.num_columns)

    @property
    def columns(self) -> List[str]:
        """Get column names."""
        if self._arrow_table is None:
            return []
        return self._arrow_table.column_names

    @property
    def arrow_schema(self) -> pa.Schema:
        """Get Arrow schema."""
        if self._arrow_table is None:
            return pa.schema([])
        return self._arrow_table.schema

    def __len__(self) -> int:
        """Get number of rows."""
        if self._arrow_table is None:
            return 0
        return self._arrow_table.num_rows

    def __getitem__(self, key):
        """Support indexing and slicing."""
        if self._arrow_table is None:
            raise IndexError("No data available")

        if isinstance(key, str):
            return self._arrow_table.column(key).to_pylist()
        elif isinstance(key, int):
            return self._arrow_table.slice(key, 1).to_pylist()[0]
        elif isinstance(key, slice):
            start, stop, step = key.indices(len(self))
            if step != 1:
                df = self.to_pandas()
                return self.__class__(data=df.iloc[key])
            else:
                sliced_table = self._arrow_table.slice(start, stop - start)
                return self.__class__.from_arrow(sliced_table)
        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def __repr__(self) -> str:
        if self._arrow_table is None:
            return f"{self.__class__.__name__}(empty)"

        return (
            f"{self.__class__.__name__}(shape={self.shape}, "
            f"columns={self.columns[:3]}{'...' if len(self.columns) > 3 else ''})"
        )


# Custom validation types - no decorator needed!
class UserProfileData(CarbonData):
    """Custom Carbon type for user profiles with validation."""

    _carbon_type_name: ClassVar[str] = "UserProfile"
    _carbon_description: ClassVar[str] = "User profile data with validation"
    _carbon_version: ClassVar[str] = "1.0"

    # Define expected fields with validation
    user_id: int = Field(..., gt=0, description="User ID must be positive")
    username: str = Field(
        ..., min_length=3, max_length=50, description="Username length 3-50 chars"
    )
    email: str = Field(
        ..., regex=r"^[\w\.-]+@[\w\.-]+\.\w+$", description="Valid email format"
    )
    age: Optional[int] = Field(None, ge=13, le=120, description="Age between 13-120")
    tags: List[str] = Field(default_factory=list, description="User tags")
    created_at: datetime = Field(
        default_factory=datetime.now, description="Creation timestamp"
    )

    @field_validator("tags")
    def validate_tags(cls, v):
        """Validate tags list."""
        if len(v) > 10:
            raise ValueError("Maximum 10 tags allowed")
        return v

    @model_validator(mode="before")
    def validate_user_data(cls, values):
        """Cross-field validation."""
        username = values.get("username", "")
        email = values.get("email", "")

        if username.lower() in email.lower():
            raise ValueError("Username should not be part of email")

        return values


class TimeSeriesData(CarbonData):
    """Custom Carbon type for time series data."""

    _carbon_type_name: ClassVar[str] = "TimeSeries"
    _carbon_description: ClassVar[str] = "Time series data with validation"
    _carbon_version: ClassVar[str] = "1.0"

    timestamps: List[datetime] = Field(..., min_items=1, description="Timestamp values")
    values: List[float] = Field(..., min_items=1, description="Numeric values")
    series_name: str = Field(..., min_length=1, description="Series identifier")
    frequency: Optional[str] = Field(
        None, regex=r"^(D|H|M|S)$", description="Frequency: D, H, M, S"
    )

    @field_validator("values")
    def validate_values(cls, v):
        """Validate numeric values."""
        if any(not isinstance(x, (int, float)) or np.isnan(x) for x in v):
            raise ValueError("All values must be valid numbers")
        return v

    @model_validator(mode="before")
    def validate_length_match(cls, values):
        """Ensure timestamps and values have same length."""
        timestamps = values.get("timestamps", [])
        vals = values.get("values", [])

        if len(timestamps) != len(vals):
            raise ValueError("Timestamps and values must have the same length")

        return values


class SalesRecordData(CarbonData):
    """Custom Carbon type for sales records."""

    _carbon_type_name: ClassVar[str] = "SalesRecord"
    _carbon_description: ClassVar[str] = "Sales transaction data"
    _carbon_version: ClassVar[str] = "1.0"

    class PaymentMethod(str, Enum):
        CASH = "cash"
        CARD = "card"
        DIGITAL = "digital"

    transaction_id: str = Field(..., min_length=8, description="Transaction ID")
    amount: float = Field(..., gt=0, description="Transaction amount > 0")
    currency: str = Field(
        default="USD", regex=r"^[A-Z]{3}$", description="3-letter currency code"
    )
    payment_method: PaymentMethod = Field(..., description="Payment method")
    customer_id: Optional[str] = Field(None, description="Customer identifier")
    items: List[Dict[str, Any]] = Field(..., min_items=1, description="Items purchased")
    transaction_date: datetime = Field(
        default_factory=datetime.now, description="Transaction timestamp"
    )

    @field_validator("items")
    def validate_items(cls, v):
        """Validate items structure."""
        required_keys = {"name", "quantity", "price"}
        for item in v:
            if not isinstance(item, dict):
                raise ValueError("Each item must be a dictionary")
            if not required_keys.issubset(item.keys()):
                raise ValueError(f"Each item must contain: {required_keys}")
            if item["quantity"] <= 0 or item["price"] <= 0:
                raise ValueError("Quantity and price must be positive")
        return v


# Example usage
if __name__ == "__main__":
    # Basic CarbonData with validation
    try:
        basic_data = CarbonData(data={"x": [1, 2, 3], "y": [4, 5, 6]})
        print(f"Basic data: {basic_data.shape}")
        print(f"Basic data type: {basic_data._carbon_type_name}")
    except ValidationError as e:
        print(f"Validation error: {e}")

    # Custom UserProfile type with validation
    try:
        user_data = UserProfileData(
            user_id=123,
            username="john_doe",
            email="john@example.com",
            age=25,
            tags=["developer", "python"],
        )
        print(f"User data validated and created: {user_data.shape}")
        print(f"User columns: {user_data.columns}")
        print(f"User type: {user_data._carbon_type_name}")

        # Serialize and deserialize
        serialized = user_data.serialize()
        deserialized = UserProfileData.deserialize(serialized)
        print(f"Deserialized user: {deserialized.shape}")

    except ValidationError as e:
        print(f"User validation error: {e}")

    # TimeSeries data
    try:
        ts_data = TimeSeriesData(
            timestamps=[datetime.now(), datetime.now()],
            values=[1.5, 2.5],
            series_name="temperature",
            frequency="H",
        )
        print(f"Time series created: {ts_data.shape}")
        print(f"Time series type: {ts_data._carbon_type_name}")
    except ValidationError as e:
        print(f"TimeSeries validation error: {e}")

    # Sales record
    try:
        sales_data = SalesRecordData(
            transaction_id="TXN123456",
            amount=99.99,
            payment_method=SalesRecordData.PaymentMethod.CARD,
            items=[{"name": "Widget", "quantity": 2, "price": 49.99}],
        )
        print(f"Sales data created: {sales_data.shape}")
        print(f"Sales type: {sales_data._carbon_type_name}")
    except ValidationError as e:
        print(f"Sales validation error: {e}")
