import pytest

def test_pagination_is_open_gap():
    from src.scrollharvest import pagination
    with pytest.raises(NotImplementedError):
        pagination.process({"id": "x"})

def test_merge_is_open_gap():
    from src.scrollharvest import merge
    with pytest.raises(NotImplementedError):
        merge.process({"id": "x"})
