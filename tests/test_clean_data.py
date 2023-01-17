# -*- coding: utf-8 -*-
from osta.clean_data import clean_data
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import copy


def test_check_names_wrong_arguments():
    df = __create_dummy_data()
    with pytest.raises(Exception):
        df = clean_data()
        with pytest.raises(Exception):
            df = clean_data(pd.DataFrame())
        with pytest.raises(Exception):
            df = clean_data(None)
        with pytest.raises(Exception):
            df = clean_data(1)
        with pytest.raises(Exception):
            df = clean_data(True)
        with pytest.raises(Exception):
            df = clean_data("yes")
        with pytest.raises(Exception):
            df = clean_data(df)


def __create_dummy_data():
    data = {"test1": ["test", "testi", "test"],
            "test2": [1, 2, 3],
            "test3": [True, False, False]
            }
    df = pd.DataFrame(data)
    return df