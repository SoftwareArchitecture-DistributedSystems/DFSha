"""Fixtures compartidas. Los helpers viven en tests/helpers.py."""
import pytest

from tests.helpers import make_client


@pytest.fixture
def user(tmp_path):
    # cada test arranca con un usuario nuevo para no arrastrar estado
    return make_client(tmp_path)


@pytest.fixture
def user2(user, tmp_path):
    """Segundo cliente del MISMO usuario. Sirve para probar locks y conflictos de versión."""
    # mismo uid pero distinto cid: para el MS son dos clientes distintos
    return make_client(tmp_path, uid=user.creds[0], cid="cliente-B")
