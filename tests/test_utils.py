#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import osta.__utils as utils
import pandas as pd


def test_utils_df():
    data = {"test1": ["test", "testi", "test"],
            "test2": [1, 2, 3],
            "test3": [True, False, False]
            }
    df = pd.DataFrame(data)
    assert utils.__is_non_empty_df(df) is True
    assert utils.__is_non_empty_df(1) is False
    assert utils.__is_non_empty_df(True) is False
    assert utils.__is_non_empty_df(None) is False
    assert utils.__is_non_empty_df("test") is False


def test_utils_percentage():
    assert utils.__is_percentage(None) is False
    assert utils.__is_percentage(True) is False
    assert utils.__is_percentage("0.5") is False
    assert utils.__is_percentage(0.5) is True
    assert utils.__is_percentage(0) is True
    assert utils.__is_percentage(1) is True


def test_utils_date():
    ser = pd.Series(["01202022", "1.2.2022", "02/2/2022"])
    assert utils.__test_if_date(ser) is True
    ser = pd.Series([True, False])
    assert utils.__test_if_date(ser) is False
    ser = pd.Series([1, 2, 3])
    assert utils.__test_if_date(ser) is False
    ser = pd.Series([None, None])
    assert utils.__test_if_date(ser) is False
    ser = pd.Series(["0122", "1.2.2", "02/22"])
    assert utils.__test_if_date(ser) is False


def test_utils_bid():
    ser = pd.Series(["0135202-4", "0135202-4", "0204819-8"])
    assert all(utils.__are_valid_bids(ser)) is True
    ser = pd.Series(["0135dd2-4", "0135ff2-4", "0135442-4"])
    assert all(utils.__are_valid_bids(ser)) is False


def test_utils_vat_number():
    ser = pd.Series(["FI01352024", "FI01352024", "FI01354424"])
    assert all(utils.__are_valid_vat_numbers(ser)) is True
    ser = pd.Series(["0135332-4", "0135332-4", "0135442-4"])
    assert all(utils.__are_valid_vat_numbers(ser)) is False


def test_utils_voucher():
    df = pd.DataFrame(["FI01352024", "FI01352024", "FI01354424"])
    assert utils.__test_if_voucher(df, 0, df.columns.tolist()) is False
    df = pd.DataFrame(["A1", "A2", "A3"])
    assert utils.__test_if_voucher(df, 0, df.columns.tolist()) is False
    df = pd.DataFrame([1001, 1002, 1002])
    assert utils.__test_if_voucher(df, 0, df.columns.tolist()) is False
    df = __create_dummy_data()
    assert utils.__test_if_voucher(df, 7, df.columns.tolist()) is True


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
