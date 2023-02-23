# -*- coding: utf-8 -*-
from osta.change_names import change_names
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import copy


def test_check_names_wrong_arguments():
    df = __create_dummy_data()
    with pytest.raises(Exception):
        df = change_names()
    with pytest.raises(Exception):
        df = change_names(pd.DataFrame())
    with pytest.raises(Exception):
        df = change_names(None)
    with pytest.raises(Exception):
        df = change_names(1)
    with pytest.raises(Exception):
        df = change_names(True)
    with pytest.raises(Exception):
        df = change_names("yes")
    with pytest.raises(Exception):
        df = change_names(df, guess_names=1)
    with pytest.raises(Exception):
        df = change_names(df, guess_names="yes")
    with pytest.raises(Exception):
        df = change_names(df, guess_names=None)
    with pytest.raises(Exception):
        df = change_names(df, make_unique=1)
    with pytest.raises(Exception):
        df = change_names(df, make_unique="yes")
    with pytest.raises(Exception):
        df = change_names(df, make_unique=None)
    with pytest.raises(Exception):
        df = change_names(df, fields=1)
    with pytest.raises(Exception):
        df = change_names(df, fields=df)
    with pytest.raises(Exception):
        df = change_names(df, fields="dict")
    with pytest.raises(Exception):
        df = change_names(df, fields=True)
    with pytest.raises(Exception):
        df = change_names(df, pattern_th=None)
    with pytest.raises(Exception):
        df = change_names(df, pattern_th=2)
    with pytest.raises(Exception):
        df = change_names(df, pattern_th=True)
    with pytest.raises(Exception):
        df = change_names(df, pattern_th="0.1")
    with pytest.raises(Exception):
        df = change_names(df, pattern_th=-0.5)
    with pytest.raises(Exception):
        df = change_names(df, scorer=None)
    with pytest.raises(Exception):
        df = change_names(df, scorer=1)
    with pytest.raises(Exception):
        df = change_names(df, scorer="None")
    with pytest.raises(Exception):
        df = change_names(df, scorer=True)
    with pytest.raises(Exception):
        df = change_names(df, match_th=2)
    with pytest.raises(Exception):
        df = change_names(df, match_th="0.1")
    with pytest.raises(Exception):
        df = change_names(df, match_th=2)
    with pytest.raises(Exception):
        df = change_names(df, match_th=-0.1)
    with pytest.raises(Exception):
        df = change_names(df, match_th=None)
    with pytest.raises(Exception):
        df = change_names(df, match_th=True)


def test_change_names_all_wrong():
    df = __create_dummy_data()
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_some_wrong():
    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan nimi", "org_number", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "Test3"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_specify_fields():
    df = __create_dummy_data()
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    own_field = dict([("test1", "suppl_name"),
                      ("test2", "testinimi"),
                      ("test3", "voucher")])
    # Do not expect a warning
    df = change_names(df, fields=own_field)
    # Expected names
    df_ref.columns = ["suppl_name", "testinimi", "voucher"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    df = __create_dummy_data()
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Create own fields as a DF
    own_field = pd.DataFrame({"key": ["Test1", "Test2", "Test3"],
                              "value": ["total", "date", "org_name"]})
    # Do not expect a warning
    df = change_names(df, fields=own_field)
    # Expected names
    df_ref.columns = ["total", "date", "org_name"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_duplicated_names():
    df = __create_dummy_data()
    # Original names
    df.columns = ["date", "Test2", "date"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["date", "voucher", "date_2"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["date", "Test2", "date"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, make_unique=False)
    # Expected names
    df_ref.columns = ["date", "voucher", "date"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_guess_names():
    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Summa."]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "total"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Summa."]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, guess_names=False)
    # Expected names
    df_ref.columns = ["Kunnan_nimi", "Kunta numero", "Summa."]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_pattern_th():
    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, pattern_th=1)
    # Expected names
    df_ref.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, pattern_th=0.6)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "total"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_test_sums():
    data = {"test1": [10.0, 12.0, 13.5],
            "test2": [1.5, 2.5, 3.0],
            "test3": [8.5, 9.5, 10.5]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["total", "vat_amount", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["total", "vat_amount", "price_ex_vat"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["total", "test2", "price_ex_vat"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["total", "vat_amount", "price_ex_vat"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["test1", "vat_amount", "price_ex_vat"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    print(df)
    print(df_ref)
    df_ref.columns = ["total", "vat_amount", "price_ex_vat"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_data_patterns():
    # matching land code, date, and voucher
    data = {"test1": ["FI", "FI", "DK", "FI"],
            "test2": ["02/01/2023", "2-1-2023", "20.12.2023", "20.1.2023"],
            "test3": [1, 2, 2, 3]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["country", "date", "voucher"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_data_patterns2():
    data = {"test1": [10, 10, 10],
            "test2": ["test", "test", "test"],
            "test3": ["0000000-0", "0000000-0", "0000000-0"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "org_name", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["org_number", "org_name", "org_id"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["org_number", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["org_number", "org_name", "org_id"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "org_id"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["org_number", "org_name", "org_id"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    data = {"test1": [10, 10, 10],
            "test2": ["test1", "test2", "test3"],
            "test3": ["0000000-0", "0204819-8", "0133226-9"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "suppl_id"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["Test1", "suppl_name", "suppl_id"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "suppl_name", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["Test1", "suppl_name", "suppl_id"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "bid"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_match_th():
    data = {"test1": [2191, 2191, 3710],
            "test2": [2282, 2251, 2191],
            "test3": ["test", "test", "test"],
            "test4": ["Henkilökohtainen apu", "test", "vammaisten perhehoito"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3", "Test4"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.67)
    # Expected names
    df_ref.columns = ["Test1", "service_cat", "Test3", "Test4"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3", "Test4"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.65)
    # Expected names
    df_ref.columns = ["service_cat", "service_cat_2",
                      "Test3", "service_cat_name"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_match_th1():
    data = {"test1": [3710, 3005, "test"],
            "test2": [3710, 3005, 3710],
            "test3": ["test", "test", "test"],
            "test4": ["myyntituotot", "test", "palkkatuki"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3", "Test4"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.67)
    # Expected names
    df_ref.columns = ["Test1", "account_number", "Test3", "Test4"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3", "Test4"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.65, make_unique=False)
    # Expected names
    df_ref.columns = ["account_name", "account_number",
                      "Test3", "account_name"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_match_th2():
    data = {"test1": [10, 12, 10],
            "test2": ["test1", "test2", "test3"],
            "test3": ["0204819-8", "0133226-9", "test"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.67)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "bid"]
    # Expect that are equal (if there are any BIDs, it is probable that column
    # includes BID values... --> match_th does not affect)
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.65)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "bid"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, pattern_th=0.6)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "total"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_match_th3():
    data = {"test1": [10, 12, 10],
            "test2": ["test1", "test2", "test3"],
            "test3": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.67)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "Test3"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.65)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "country"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, pattern_th=0.6)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "total"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def test_change_names_match_th4():
    data = {"test1": ["FI00009578", "BE4448596870", "test"],
            "test2": ["test1", "test2", "test3"],
            "test3": ["FI", "FI", "test"]
            }
    df = pd.DataFrame(data)
    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.67)
    # Expected names
    df_ref.columns = ["Test1", "Test2", "Test3"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    # Original names
    df.columns = ["Test1", "Test2", "Test3"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, match_th=0.65)
    # Expected names
    df_ref.columns = ["vat_number", "Test2", "country"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)

    df = __create_dummy_data()
    # Original names
    df.columns = ["Kunnan_nimi", "Kunta numero", "Kokko.summa"]
    df_ref = copy.copy(df)
    # Expect a warning
    with pytest.warns(Warning):
        df = change_names(df, pattern_th=0.6)
    # Expected names
    df_ref.columns = ["org_name", "org_number", "total"]
    # Expect that are equal
    assert_frame_equal(df, df_ref)


def __create_dummy_data():
    data = {"test1": ["test", "testi", "test"],
            "test2": [1, 2, 3],
            "test3": [True, False, False]
            }
    df = pd.DataFrame(data)
    return df
