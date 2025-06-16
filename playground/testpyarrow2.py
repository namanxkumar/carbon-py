import io
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa


class CarbonData:
    """
    Represents data passed between modules in the Carbon framework.
    Is able to serialize and deserialize data for communication over the Apache Arrow protocol.
    """

    def __init__(self, data: Any = None, schema: Optional[pa.Schema] = None):
        """
        Initialize CarbonData with various data types.

        Args:
            data: The data to wrap (dict, list, pandas DataFrame, numpy array, etc.)
            schema: Optional Arrow schema for validation
        """
        self._arrow_table = None
        self._metadata = {}

        if data is not None:
            self._arrow_table = self._convert_to_arrow(data, schema)

    def _convert_to_arrow(
        self, data: Any, schema: Optional[pa.Schema] = None
    ) -> pa.Table:
        """Convert various data types to Arrow Table."""

        if isinstance(data, pa.Table):
            return data
        elif isinstance(data, pa.RecordBatch):
            return pa.Table.from_batches([data])
        elif isinstance(data, pd.DataFrame):
            return pa.Table.from_pandas(data, schema=schema)
        elif isinstance(data, np.ndarray):
            if data.ndim == 1:
                # 1D array - convert to single column table
                return pa.table([data], names=["values"])
            elif data.ndim == 2:
                # 2D array - each column becomes a table column
                columns = [data[:, i] for i in range(data.shape[1])]
                names = [f"col_{i}" for i in range(data.shape[1])]
                return pa.table(columns, names=names)
            else:
                raise ValueError("Arrays with more than 2 dimensions not supported")
        elif isinstance(data, dict):
            # Handle nested dictionaries and various value types
            return self._dict_to_arrow(data)
        elif isinstance(data, list):
            # Handle list of dictionaries (records) or simple lists
            if data and isinstance(data[0], dict):
                return pa.Table.from_pylist(data)
            else:
                return pa.table([data], names=["values"])
        else:
            # Try to convert single values or other types
            return pa.table([[data]], names=["values"])

    def _dict_to_arrow(self, data: Dict) -> pa.Table:
        """Convert dictionary to Arrow table, handling nested structures."""
        flattened = {}

        for key, value in data.items():
            if isinstance(value, (list, np.ndarray)):
                flattened[key] = value
            elif isinstance(value, dict):
                # Flatten nested dictionaries
                for nested_key, nested_value in value.items():
                    flattened[f"{key}.{nested_key}"] = [nested_value]
            else:
                flattened[key] = [value]

        return pa.table(flattened)

    @classmethod
    def from_arrow(cls, arrow_data: Union[pa.Table, pa.RecordBatch]) -> "CarbonData":
        """Create CarbonData from Arrow data."""
        instance = cls()
        if isinstance(arrow_data, pa.RecordBatch):
            instance._arrow_table = pa.Table.from_batches([arrow_data])
        else:
            instance._arrow_table = arrow_data
        return instance

    @classmethod
    def from_json(cls, json_str: str) -> "CarbonData":
        """Create CarbonData from JSON string."""
        data = json.loads(json_str)
        return cls(data)

    @classmethod
    def from_csv(cls, csv_path: str, **kwargs) -> "CarbonData":
        """Create CarbonData from CSV file."""
        df = pd.read_csv(csv_path, **kwargs)
        return cls(df)

    @classmethod
    def from_parquet(cls, parquet_path: str) -> "CarbonData":
        """Create CarbonData from Parquet file."""
        table = pa.parquet.read_table(parquet_path)
        return cls.from_arrow(table)

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
        """Convert to list of dictionaries (records)."""
        if self._arrow_table is None:
            return []
        return self._arrow_table.to_pylist()

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)

    def serialize(self) -> bytes:
        """Serialize to bytes using Arrow IPC format."""
        if self._arrow_table is None:
            return b""

        buffer = io.BytesIO()
        with pa.ipc.new_stream(buffer, self._arrow_table.schema) as writer:
            writer.write_table(self._arrow_table)
        return buffer.getvalue()

    @classmethod
    def deserialize(cls, data: bytes) -> "CarbonData":
        """Deserialize from bytes using Arrow IPC format."""
        buffer = io.BytesIO(data)
        reader = pa.ipc.open_stream(buffer)
        table = reader.read_all()
        return cls.from_arrow(table)

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

    def filter(self, condition) -> "CarbonData":
        """Filter data based on condition."""
        if self._arrow_table is None:
            return CarbonData()

        # Allow pandas-style filtering
        if callable(condition):
            df = self.to_pandas()
            filtered_df = df[condition(df)]
            return CarbonData(filtered_df)
        else:
            # Arrow compute expression
            filtered_table = self._arrow_table.filter(condition)
            return CarbonData.from_arrow(filtered_table)

    def select(self, columns: List[str]) -> "CarbonData":
        """Select specific columns."""
        if self._arrow_table is None:
            return CarbonData()

        selected_table = self._arrow_table.select(columns)
        return CarbonData.from_arrow(selected_table)

    def sort(self, by: Union[str, List[str]], ascending: bool = True) -> "CarbonData":
        """Sort data by column(s)."""
        if self._arrow_table is None:
            return CarbonData()

        if isinstance(by, str):
            by = [by]

        sort_keys = [(col, "ascending" if ascending else "descending") for col in by]
        sorted_table = self._arrow_table.sort_by(sort_keys)
        return CarbonData.from_arrow(sorted_table)

    def group_by(self, columns: Union[str, List[str]]) -> "CarbonGroupBy":
        """Group data by column(s)."""
        if isinstance(columns, str):
            columns = [columns]
        return CarbonGroupBy(self._arrow_table, columns)

    def join(
        self, other: "CarbonData", keys: Union[str, List[str]], how: str = "inner"
    ) -> "CarbonData":
        """Join with another CarbonData object."""
        if self._arrow_table is None or other._arrow_table is None:
            return CarbonData()

        # Convert to pandas for complex joins (Arrow join support is limited)
        left_df = self.to_pandas()
        right_df = other.to_pandas()

        joined_df = left_df.merge(right_df, on=keys, how=how)
        return CarbonData(joined_df)

    def add_metadata(self, key: str, value: Any):
        """Add metadata to the data."""
        self._metadata[key] = value

    def get_metadata(self, key: str) -> Any:
        """Get metadata value."""
        return self._metadata.get(key)

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
    def schema(self) -> pa.Schema:
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
            # Column selection
            return self._arrow_table.column(key).to_pylist()
        elif isinstance(key, int):
            # Row selection
            return self._arrow_table.slice(key, 1).to_pylist()[0]
        elif isinstance(key, slice):
            # Row slicing
            start, stop, step = key.indices(len(self))
            if step != 1:
                # For step != 1, convert to pandas
                df = self.to_pandas()
                return CarbonData(df.iloc[key])
            else:
                sliced_table = self._arrow_table.slice(start, stop - start)
                return CarbonData.from_arrow(sliced_table)
        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def __repr__(self) -> str:
        if self._arrow_table is None:
            return "CarbonData(empty)"

        return (
            f"CarbonData(shape={self.shape}, "
            f"columns={self.columns[:3]}{'...' if len(self.columns) > 3 else ''})"
        )


class CarbonGroupBy:
    """Handle grouped operations on CarbonData."""

    def __init__(self, arrow_table: pa.Table, group_columns: List[str]):
        self.arrow_table = arrow_table
        self.group_columns = group_columns

    def count(self) -> "CarbonData":
        """Count rows in each group."""
        df = self.arrow_table.to_pandas()
        result = df.groupby(self.group_columns).size().reset_index(name="count")
        return CarbonData(result)

    def sum(self, columns: Optional[List[str]] = None) -> "CarbonData":
        """Sum numeric columns for each group."""
        df = self.arrow_table.to_pandas()
        grouped = df.groupby(self.group_columns)

        if columns:
            result = grouped[columns].sum().reset_index()
        else:
            result = grouped.sum(numeric_only=True).reset_index()

        return CarbonData(result)

    def mean(self, columns: Optional[List[str]] = None) -> "CarbonData":
        """Calculate mean for each group."""
        df = self.arrow_table.to_pandas()
        grouped = df.groupby(self.group_columns)

        if columns:
            result = grouped[columns].mean().reset_index()
        else:
            result = grouped.mean(numeric_only=True).reset_index()

        return CarbonData(result)


# Example usage and utility functions
def create_sample_data():
    """Create sample data for testing."""
    import random

    data = {
        "id": list(range(1000)),
        "category": [random.choice(["A", "B", "C"]) for _ in range(1000)],
        "value": [random.uniform(0, 100) for _ in range(1000)],
        "timestamp": [datetime.now() for _ in range(1000)],
    }
    return CarbonData(data)


# Example usage
if __name__ == "__main__":
    # Create data from various sources
    carbon_data = CarbonData({"x": [1, 2, 3], "y": [4, 5, 6]})
    print(f"Original: {carbon_data}")

    # Serialize and deserialize
    serialized = carbon_data.serialize()
    deserialized = CarbonData.deserialize(serialized)
    print(f"Deserialized: {deserialized}")

    # Convert to different formats
    df = carbon_data.to_pandas()
    print(f"As DataFrame:\n{df}")

    # Filtering and operations
    sample = create_sample_data()
    filtered = sample.filter(lambda df: df["value"] > 50)
    grouped = sample.group_by("category").mean(["value"])

    print(f"Sample data shape: {sample.shape}")
    print(f"Filtered shape: {filtered.shape}")
    print(f"Grouped means:\n{grouped.to_pandas()}")
