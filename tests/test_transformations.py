"""
Author: Mohamed Chaari
"""
import pytest
from datetime import datetime
import pandas as pd
import pandera as pa

# Test pandera schema from batch/jobs/daily_etl.py
def test_pandera_schema_valid():
    """Test the daily ETL pandera schema against valid data."""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '../batch/jobs'))
    from daily_etl import schema

    data = pd.DataFrame({
        "symbol": ["BTC", "ETH"],
        "window_start": [pd.Timestamp("2024-01-01 00:00:00"), pd.Timestamp("2024-01-01 00:01:00")],
        "window_end": [pd.Timestamp("2024-01-01 00:01:00"), pd.Timestamp("2024-01-01 00:02:00")],
        "open": [40000.0, 2000.0],
        "high": [40100.0, 2010.0],
        "low": [39900.0, 1990.0],
        "close": [40050.0, 2005.0],
        "avg_volume": [100.0, 500.0]
    })

    # Should not raise exception
    validated = schema.validate(data)
    assert not validated.empty

def test_pandera_schema_invalid():
    """Test the daily ETL pandera schema against invalid data."""
    import sys
    import os
    sys.path.append(os.path.join(os.path.dirname(__file__), '../batch/jobs'))
    from daily_etl import schema

    data = pd.DataFrame({
        "symbol": ["INVALID_COIN"],
        "window_start": [pd.Timestamp("2024-01-01 00:00:00")],
        "window_end": [pd.Timestamp("2024-01-01 00:01:00")],
        "open": [-10.0], # Invalid negative price
        "high": [40100.0],
        "low": [39900.0],
        "close": [40050.0],
        "avg_volume": [100.0]
    })

    with pytest.raises(pa.errors.SchemaError):
        schema.validate(data)
