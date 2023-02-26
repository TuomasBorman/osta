# -*- coding: utf-8 -*-
from osta.clean_data import clean_data
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import pkg_resources


def test_check_names_wrong_arguments():
    df = __create_dummy_data()
    # Some tests give warnings...
    with pytest.warns(Warning):
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
            df = clean_data(df, service_data="test_file")
        # date_fomat does not raise errors, pandas can take different values
        # and it is fed to it.
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
            df = clean_data(df, thresh=True)
        with pytest.raises(Exception):
            df = clean_data(df, thresh="0.5")
        with pytest.raises(Exception):
            df = clean_data(df, thresh=None)


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
    data = {"org_number": ["484", "484", 424],
            "org_name": ["Merikarvia", "Merikarvia", "test3"],
            "org_id": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"org_number": [48344, 444484, 424],
            "org_name": ["Akaa", "Alajärvi", "test3"],
            "org_id": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"org_number": ["020", "005", 424],
            "org_name": ["Akaa", "Alajärvi", "test3"],
            "org_id": ["2050864-5", "0177619-3", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"org_number": ["484", "484", "424"],
            "org_name": ["test1", "test2", "test3"],
            "org_id": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_org=True)
    # Expected names
    data = {"org_number": ["484", "484", "424"],
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
            "org_name": ["test", "Akaa", "test3"],
            "org_id": ["0135202-4", "013test4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        file = pkg_resources.resource_filename("osta", "resources/" +
                                               "municipality_codes.csv")
        df = clean_data(df, org_data=pd.read_csv(file, index_col=0))
    # Expected names
    data = {"org_number": [484, 20, 4234344],
            "org_name": ["Merikarvia", "Akaa", "test3"],
            "org_id": ["0135202-4", "2050864-5", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_suppl():
    data = {"suppl_number": [4844, 4833544, 4234344],
            "suppl_name": ["test", "test", "test3"],
            "suppl_id": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df, disable_org=True)
    # Expected names
    data = {"suppl_number": [4844, 4833544, 4234344],
            "suppl_name": ["test", "test", "test3"],
            "suppl_id": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"suppl_number": [4844, 48344, 4844444],
            "suppl_name": ["test", "test1", "test3"],
            "suppl_id": ["01242425", "test243", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_suppl=True)
    # Expected names
    data = {"suppl_number": [4844, 48344, 4844444],
            "suppl_name": ["test", "test1", "test3"],
            "suppl_id": ["01242425", "test243", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"suppl_number": [4844, 48344, 4844444],
            "suppl_name": ["test", "test1", "test3"],
            "suppl_id": ["01242425", "test243", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"suppl_number": [4844, 48344, 4844444],
            "suppl_name": ["test", "test1", "test3"],
            "suppl_id": ["01242425", "test243", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"suppl_number": [4844, 48344, 4234344],
            "suppl_name": ["test", "Merikarvia", "test3"],
            "vat_number": ["FI01352024", "Ftest2024", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        file = pkg_resources.resource_filename("osta", "resources/" +
                                               "municipality_codes.csv")
        df = clean_data(df, suppl_data=pd.read_csv(file, index_col=0))
    # Expected names
    data = {"suppl_number": [484, 484, 4234344],
            "suppl_name": ["Merikarvia", "Merikarvia", "test3"],
            "vat_number": ["FI01352024", "FI01352024", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_service():
    data = {"service_cat": [3051, 3701, 3701],
            "service_cat_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"service_cat": [3051, 3701, 3701],
            "service_cat_name": ["Perusopetus", "Museopalvelut",
                                 "Museopalvelut"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"service_cat": [2701, 26663, 6901],
            "service_cat_name": ["test", "Esiopetus", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    df = clean_data(df, db_year=2022)
    # Expected names
    data = {"service_cat": [2701, 3041, 6901],
            "service_cat_name": ["Elintarvikevalvonta ja -neuvonta",
                                 "Esiopetus",
                                 "Löytöeläimet"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"service_cat": [2701, 274301, 4102],
            "service_cat_name": ["test", "Nuorisopalvelut", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"service_cat": [2701, 3601, 4102],
            "service_cat_name": ["Elintarvikevalvonta ja -neuvonta",
                                 "Nuorisopalvelut",
                                 "Yleiskaavoitus"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"service_cat": [6901, 273431, 6901],
            "service_cat_name": ["test", "Es45tus", "löytöeläimet"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_service=True)
    # Expected names
    data = {"service_cat": [6901, 273431, 6901],
            "service_cat_name": ["test", "Es45tus", "löytöeläimet"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"service_cat": [2701, 274301, 4102],
            "service_cat_name": ["test", "Nuorisopalvelut", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    file = pkg_resources.resource_filename("osta", "resources/" +
                                           "service_codes.csv")
    df = clean_data(df, service_data=pd.read_csv(file, index_col=0))
    # Expected names
    data = {"service_cat": [2701, 3601, 4102],
            "service_cat_name": ["Elintarvikevalvonta ja -neuvonta",
                                 "Nuorisopalvelut",
                                 "Yleiskaavoitus"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_account():
    data = {"account_number": [1020, 1187, 370111],
            "account_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning; not all are found
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"account_number": [1020, 1187, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning; not all are found
    with pytest.warns(Warning):
        df = clean_data(df, db_year=2021)
    # Expected names
    data = {"account_number": [1020, 1187, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_account=True)
    # Expected names
    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["ennakkomaksut", "muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    file = pkg_resources.resource_filename("osta", "resources/" +
                                           "account_info.csv")
    # Expect a warning; not all are found
    with pytest.warns(Warning):
        df = clean_data(df, account_data_data=pd.read_csv(file, index_col=0))
    # Expected names
    data = {"account_number": [1020, 1187, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    # Duplicated names --> because no match, not change
    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df.columns = ["account_number", "account_name", "account_name"]
    # Expect a warning; not all are found and duplicated names
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"account_number": [102460, 1184667, 370111],
            "account_name": ["Ennakkomaksut", "Muut hyödykkeet", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }


def test_clean_data_voucher():
    data = {"voucher": [104420, 104420, 104420, 104420, 104420,
                        104420, 104420, 104420, 104420, 104410],
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"voucher": [100, 110, 110],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"voucher": [100, 110, 110],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"voucher": ["A2000", "A2000", "A2001"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"voucher": ["A2000", "A2000", "A2001"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_country():
    data = {"country": ["FI", "FI", "Denmark"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"country": ["FI", "FI", "DK"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"country": ["FI", "Sweden", "Denmark"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df, country_format="name_fin")
    # Expected names
    data = {"country": ["Suomi", "Ruotsi", "Tanska"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"country": ["FI", "Sweden", "Denmark"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning because values are still matched with supplier data
    with pytest.warns(Warning):
        df = clean_data(df, disable_country=True)
    # Expected names
    data = {"country": ["FI", "Sweden", "Denmark"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_date():
    data = {"date": ["10.h.2013", "12.12-12", "12.12.12"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"date": ["10.h.2013", "12.12-12", "12.12.12"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"date": ["31.2.16", "12.12.21", "01.02.22"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    # Expect a warning since one date is not correct
    with pytest.warns(Warning):
        df = clean_data(df, date_format="%d.%m.%Y")
    # Expected names
    data = {"date": [None, "12.12.2021", "01.02.2022"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"date": ["28.3.16", "12.12.21", "01.02.22"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, date_format="%d.%m.%Y")
    # Expected names
    data = {"date": ["28.03.2016", "12.12.2021", "01.02.2022"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"date": ["01122013", "31122012", "20022012"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, date_format="%Y/%d/%m")
    # Expected names
    data = {"date": ["2013/01/12", "2012/31/12", "2012/20/02"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    print(df)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"date": ["01122013", "3122012", "200212"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_date=True)
    df_expect = pd.DataFrame(data)
    # Expected names
    data = {"date": ["01122013", "3122012", "200212"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_vat_number():
    data = {"vat_number": ["FI01352024", "FI01352024", "test"],
            "suppl_name": ["test", "test", "test3"],
            "country": ["FI", "DK", "FI"]
            }
    df = pd.DataFrame(data)
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"vat_number": ["FI01352024", "FI01352024", "test"],
            "suppl_name": ["test", "test", "test3"],
            "country": ["FI", "DK", "FI"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_number": ["FI01352024", "FI01352024", "test"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_suppl=True)
    # Expected names
    data = {"vat_number": ["FI01352024", "FI01352024", "test"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_number": ["FI01352024", "FI01352024", "FI02252024"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"vat_number": ["FI01352024", "FI01352024", "FI02252024"],
            "suppl_name": ["test", "test", "test3"],
            "test": ["0135202-4", "0135202-4", "test"]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_clean_data_sums():
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3, 3, 4]
            }
    df = pd.DataFrame(data)
    # Expect warning
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3.0, 3.0, 4.0]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3, 3, 4]
            }
    df = pd.DataFrame(data)
    df = clean_data(df, disable_sums=True)
    # Expected names
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3, 3, 4]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3.0, 4.0, 3.0]
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            "total": [3.0, 4.0, 3.0]
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_amount": ["2.0", "1.0", "1.5"],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df = pd.DataFrame(data)
    df = clean_data(df)
    # Expected names
    data = {"vat_amount": [2.0, 1.0, 1.5],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)

    data = {"vat_amount": ["2df.0", "1gg.0", "1.5"],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df = pd.DataFrame(data)
    with pytest.warns(Warning):
        df = clean_data(df)
    # Expected names
    data = {"vat_amount": ["2df.0", "1gg.0", "1.5"],
            "price_ex_vat": [1, 3.0, 1.5],
            }
    df_expect = pd.DataFrame(data)
    # Expect that are equal
    assert_frame_equal(df, df_expect)


def test_thresh():
    def test_clean_data_sums():
        data = {"vat_amount": [2.0, "", 1.5],
                "price_ex_vat": [1, 3.0, 1.5],
                "total": [3, 3, 4]
                }
        df = pd.DataFrame(data)
        # Expect warning
        with pytest.warns(Warning):
            df = clean_data(df, thresh=2)
        # Expected names
        data = {"vat_amount": [2.0, 1.5],
                "price_ex_vat": [1, 1.5],
                "total": [3.0, 4.0]
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
