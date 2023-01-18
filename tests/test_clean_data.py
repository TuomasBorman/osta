# -*- coding: utf-8 -*-
from osta.clean_data import clean_data
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import pkg_resources


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


def test_clean_data_org():
    data = {"org_number": [48344, 484, 424],
            "org_name": ["Merikarvian kunta", "test2", "test3"],
            "org_id": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"org_number": [484, 484, 424],
            "org_name": ["Merikarvia", "Merikarvia", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"org_number": [484, 484, 424],
            "org_name": ["test1", "test2", "test3"],
            "org_id": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_org=True)
    # Expected names
    data = {"org_number": [484, 484, 424],
            "org_name": ["test1", "test2", "test3"],
            "org_id": ["FI", "FI", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"org_number": [4844, 48344, 4234344],
            "org_name": ["test", "test", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_org=True)
    # Expected names
    data = {"org_number": [4844, 48344, 4234344],
            "org_name": ["test", "test", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"org_number": [4844, 48344, 4234344],
            "org_name": ["test", "test", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        file = pkg_resources.resource_filename("osta", "resources/" +
                                               "municipality_codes.csv")
        df = clean_data(df, org_data=pd.read_csv(file, index_col=0))
    # Expected names
    data = {"org_number": [484, 484, 4234344],
            "org_name": ["Merikarvia", "Merikarvia", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def __create_dummy_data():
    data = {"org_name": ["test", "testi", "test"],
            "org_number": [1, 2, 3],
            "org_id": ["test", "testi", "test"],
            "date": ["02.04.2023", "02.10.2023", "23.06.2022"],
            "suppl_name": ["test", "testi", "test"],
            "total": [1.10, 2.04, 3.74],
            "test3": [True, False, False],
            "voucher": [1, 2, 3],
            "account_number": [123, 123, 434],
            "service_cat": [123, 123, 123],
            "vat_number": ["test", "testi", "test"],
            "country": ["test", "testi", "test"],
            }
    df = pd.DataFrame(data)
    return df
