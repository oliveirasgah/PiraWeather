"""
Root conftest.py — runs before any test collection.

Sets environment variables required by pydantic-settings and mocks
Streamlit (and Plotly) so dashboard/app.py can be imported in a plain
pytest session without a running Streamlit server.
"""

import os
import sys
from unittest.mock import MagicMock

# Must be set before any module-level Settings() is instantiated.
os.environ.setdefault("POSTGRES_USER", "test_user")
os.environ.setdefault("POSTGRES_PASSWORD", "test_password")


# ---------------------------------------------------------------------------
# Streamlit mock
#
# st.cache_data / st.cache_resource are replaced with identity decorators so
# that the decorated functions remain their original selves and can be called
# directly in tests.
# ---------------------------------------------------------------------------

def _passthrough_decorator(*args, **kwargs):
    """Acts as @decorator or @decorator(...)  — always returns the function."""
    if len(args) == 1 and callable(args[0]):
        return args[0]
    return lambda fn: fn


_st_mock = MagicMock()
_st_mock.cache_data.side_effect = _passthrough_decorator
_st_mock.cache_resource.side_effect = _passthrough_decorator
# dashboard/app.py does: tab_bronze, tab_analytics = st.tabs([...])
_st_mock.tabs.return_value = (MagicMock(), MagicMock())

sys.modules["streamlit"] = _st_mock
sys.modules["plotly"] = MagicMock()
sys.modules["plotly.express"] = MagicMock()
