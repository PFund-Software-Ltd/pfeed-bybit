import pfeed as pe
import pytest


@pytest.fixture
def bybit(request):
    return pe.Bybit(**getattr(request, "param", {}))
