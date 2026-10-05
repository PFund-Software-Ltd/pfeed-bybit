import pytest

import pfeed as pe


@pytest.fixture
def bybit(request):
    return pe.Bybit(**getattr(request, "param", {}))
