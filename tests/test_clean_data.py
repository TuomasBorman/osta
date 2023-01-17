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
            df = clean_data(df, disable_org="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_org=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_org=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_org=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_suppl="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_suppl=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_suppl=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_suppl=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_sums="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_sums=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_sums=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_sums=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_country="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_country=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_country=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_country=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_date="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_date=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_date=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_date=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_vat_number="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_vat_number=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_vat_number=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_vat_number=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_voucher="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_voucher=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_voucher=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_voucher=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_service="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_service=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_service=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_service=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, disable_account="yes")
        with pytest.raises(Exception):
            df = clean_data(df, disable_account=1)
        with pytest.raises(Exception):
            df = clean_data(df, disable_account=None)
        with pytest.raises(Exception):
            df = clean_data(df, disable_account=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, org_data=True)
        with pytest.raises(Exception):
            df = clean_data(df, org_data=0)
        with pytest.raises(Exception):
            df = clean_data(df, org_data="test_file")
        with pytest.raises(Exception):
            df = clean_data(df, suppl_data=True)
        with pytest.raises(Exception):
            df = clean_data(df, suppl_data=0)
        with pytest.raises(Exception):
            df = clean_data(df, suppl_data="test_file")
        with pytest.raises(Exception):
            df = clean_data(df, account_data=True)
        with pytest.raises(Exception):
            df = clean_data(df, account_data=0)
        with pytest.raises(Exception):
            df = clean_data(df, account_data="test_file")
        with pytest.raises(Exception):
            df = clean_data(df, service_data=True)
        with pytest.raises(Exception):
            df = clean_data(df, service_data=0)
        with pytest.raises(Exception):
            df = clean_data(df, service="test_file")
        with pytest.raises(Exception):
            df = clean_data(df, date_format="test")
        with pytest.raises(Exception):
            df = clean_data(df, date_format=True)
        with pytest.raises(Exception):
            df = clean_data(df, date_format=1)
        with pytest.raises(Exception):
            df = clean_data(df, country_format="test")
        with pytest.raises(Exception):
            df = clean_data(df, country_format=True)
        with pytest.raises(Exception):
            df = clean_data(df, country_format=1)
        with pytest.raises(Exception):
            df = clean_data(df, dayfirst="test")
        with pytest.raises(Exception):
            df = clean_data(df, dayfirst=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, dayfirst=1)
        with pytest.raises(Exception):
            df = clean_data(df, yearfirst="test")
        with pytest.raises(Exception):
            df = clean_data(df, yearfirst=[True, True])
        with pytest.raises(Exception):
            df = clean_data(df, yearfirst=1)
        with pytest.raises(Exception):
            df = clean_data(df, db_year="test")
        with pytest.raises(Exception):
            df = clean_data(df, db_year=True)
        with pytest.raises(Exception):
            df = clean_data(df, db_year=1)
        with pytest.raises(Exception):
            df = clean_data(df, pattern_th=10)
        with pytest.raises(Exception):
            df = clean_data(df, pattern_th=True)
        with pytest.raises(Exception):
            df = clean_data(df, pattern_th="0.5")
        with pytest.raises(Exception):
            df = clean_data(df, pattern_th=None)


def __create_dummy_data():
    data = {"test1": ["test", "testi", "test"],
            "test2": [1, 2, 3],
            "test3": [True, False, False]
            }
    df = pd.DataFrame(data)
    return df