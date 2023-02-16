#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from osta.enrich_data import enrich_data
from osta.enrich_data import fetch_company_data
from osta.enrich_data import fetch_financial_data
from osta.enrich_data import fetch_org_company_data
from osta.enrich_data import fetch_org_data
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
import pkg_resources


def test_enrich_data_wrong_arguments():
    df = __create_dummy_data()
    with pytest.raises(Exception):
        enrich_data()
    with pytest.raises(Exception):
        enrich_data(pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(1)
    with pytest.raises(Exception):
        enrich_data("test")
    with pytest.raises(Exception):
        enrich_data(True)
    with pytest.raises(Exception):
        enrich_data(df, org_data=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, org_data=1)
    with pytest.raises(Exception):
        enrich_data(df, org_data="test")
    with pytest.raises(Exception):
        enrich_data(df, org_data=True)
    with pytest.raises(Exception):
        enrich_data(df, suppl_data=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, suppl_data=1)
    with pytest.raises(Exception):
        enrich_data(df, suppl_data="test")
    with pytest.raises(Exception):
        enrich_data(df, suppl_data=True)
    with pytest.raises(Exception):
        enrich_data(df, service_data=[pd.DataFrame()])
    with pytest.raises(Exception):
        enrich_data(df, service_data=1)
    with pytest.raises(Exception):
        enrich_data(df, service_data="test")
    with pytest.raises(Exception):
        enrich_data(df, service_data=True)
    with pytest.raises(Exception):
        enrich_data(df, account_data=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, account_data=1)
    with pytest.raises(Exception):
        enrich_data(df, account_data="test")
    with pytest.raises(Exception):
        enrich_data(df, account_data=True)
    with pytest.raises(Exception):
        enrich_data(df, disable_org=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, disable_org=1)
    with pytest.raises(Exception):
        enrich_data(df, disable_org="test")
    with pytest.raises(Exception):
        enrich_data(df, disable_org=[True])
    with pytest.raises(Exception):
        enrich_data(df, disable_suppl=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, disable_suppl=1)
    with pytest.raises(Exception):
        enrich_data(df, disable_suppl="test")
    with pytest.raises(Exception):
        enrich_data(df, disable_suppl=[True])
    with pytest.raises(Exception):
        enrich_data(df, disable_service=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, disable_service=None)
    with pytest.raises(Exception):
        enrich_data(df, disable_service="test")
    with pytest.raises(Exception):
        enrich_data(df, disable_service=[True])
    with pytest.raises(Exception):
        enrich_data(df, disable_account=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, disable_account=None)
    with pytest.raises(Exception):
        enrich_data(df, disable_account="test")
    with pytest.raises(Exception):
        enrich_data(df, disable_account=[True])
    with pytest.raises(Exception):
        enrich_data(df, disable_sums=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, disable_sums=None)
    with pytest.raises(Exception):
        enrich_data(df, disable_sums="test")
    with pytest.raises(Exception):
        enrich_data(df, disable_sums=[True])
    with pytest.raises(Exception):
        enrich_data(df, subset_account_data=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, subset_account_data=None)
    with pytest.raises(Exception):
        enrich_data(df, subset_account_data="test")
    with pytest.raises(Exception):
        enrich_data(df, subset_account_data=[True])
    with pytest.raises(Exception):
        enrich_data(df, db_year=pd.DataFrame())
    with pytest.raises(Exception):
        enrich_data(df, db_year=1920)
    with pytest.raises(Exception):
        enrich_data(df, db_year="test")
    with pytest.raises(Exception):
        enrich_data(df, db_year=[True])


def test_fetch_company_data_wrong_arguments():
    df = __create_dummy_data()
    bids = pd.Series(["1458359-3", "2403929-2"])
    with pytest.raises(Exception):
        fetch_company_data()
    with pytest.raises(Exception):
        fetch_company_data(df)
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_company_data(pd.Series())
    with pytest.raises(Exception):
        fetch_company_data(1)
    with pytest.raises(Exception):
        fetch_company_data(True)
    with pytest.raises(Exception):
        fetch_company_data([1, 2])
    with pytest.raises(Exception):
        fetch_company_data(None)
    with pytest.raises(Exception):
        fetch_company_data(bids, language="test")
    with pytest.raises(Exception):
        fetch_company_data(bids, language=1)
    with pytest.raises(Exception):
        fetch_company_data(bids, language=True)
    with pytest.raises(Exception):
        fetch_company_data(bids, language=None)
    with pytest.raises(Exception):
        fetch_company_data(bids, only_ltd="test")
    with pytest.raises(Exception):
        fetch_company_data(bids, only_ltd=1)
    with pytest.raises(Exception):
        fetch_company_data(bids, only_ltd=None)
    with pytest.raises(Exception):
        fetch_company_data(bids, merge_bid="test")
    with pytest.raises(Exception):
        fetch_company_data(bids, merge_bid=1)
    with pytest.raises(Exception):
        fetch_company_data(bids, merge_bid=None)
    with pytest.raises(Exception):
        fetch_company_data(bids, use_cache="test")
    with pytest.raises(Exception):
        fetch_company_data(bids, use_cache=1)
    with pytest.raises(Exception):
        fetch_company_data(bids, use_cache=None)
    with pytest.raises(Exception):
        fetch_company_data(bids, temp_dir=["test"])
    with pytest.raises(Exception):
        fetch_company_data(bids, temp_dir=1)
    with pytest.raises(Exception):
        fetch_company_data(bids, temp_dir=True)


def test_fetch_financial_data_wrong_arguments():
    df = __create_dummy_data()
    codes = pd.Series(["0135202-4", "1567535-0"])
    years = pd.Series(["2021", "2022"])
    with pytest.raises(Exception):
        fetch_financial_data()
    with pytest.raises(Exception):
        fetch_financial_data(df)
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_financial_data(pd.Series())
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_financial_data(codes, pd.Series())
    with pytest.raises(Exception):
        fetch_financial_data(1)
    with pytest.raises(Exception):
        fetch_financial_data("TEST")
    with pytest.raises(Exception):
        fetch_financial_data(codes)
    with pytest.raises(Exception):
        fetch_financial_data(codes, "test")
    with pytest.raises(Exception):
        fetch_financial_data(codes, True)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, subset=1)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, subset="test")
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, subset=None)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, wide_format=1)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, wide_format="test")
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, wide_format=None)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, language=1)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, language="test")
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, language=None)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, rename_cols=1)
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, rename_cols="test")
    with pytest.raises(Exception):
        fetch_financial_data(codes, years, rename_cols=None)


def test_fetch_org_company_data_wrong_arguments():
    df = __create_dummy_data()
    codes = pd.Series(["0135202-4", "1567535-0"])
    years = pd.Series(["2021", "2022"])
    with pytest.raises(Exception):
        fetch_org_company_data()
    with pytest.raises(Exception):
        fetch_org_company_data(df)
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_org_company_data(pd.Series())
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_org_company_data(codes, pd.Series())
    with pytest.raises(Exception):
        fetch_org_company_data(1)
    with pytest.raises(Exception):
        fetch_org_company_data("TEST")
    with pytest.raises(Exception):
        fetch_org_company_data(codes)
    with pytest.raises(Exception):
        fetch_org_company_data(codes, "test")
    with pytest.raises(Exception):
        fetch_org_company_data(codes, True)
    with pytest.raises(Exception):
        fetch_org_company_data(codes, years, rename_cols=1)
    with pytest.raises(Exception):
        fetch_org_company_data(codes, years, rename_cols="test")
    with pytest.raises(Exception):
        fetch_org_company_data(codes, years, rename_cols=None)


def test_fetch_org_data_wrong_arguments():
    df = __create_dummy_data()
    codes = pd.Series(["020", "005"])
    years = pd.Series(["2021", "2022"])
    with pytest.raises(Exception):
        fetch_org_data()
    with pytest.raises(Exception):
        fetch_org_data(df)
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_org_data(pd.Series())
    with pytest.raises(Exception):
        with pytest.warns(Warning):
            fetch_org_data(codes, pd.Series())
    with pytest.raises(Exception):
        fetch_org_data(1)
    with pytest.raises(Exception):
        fetch_org_data("TEST")
    with pytest.raises(Exception):
        fetch_org_data(codes)
    with pytest.raises(Exception):
        fetch_org_data(codes, "test")
    with pytest.raises(Exception):
        fetch_org_data(codes, True)
    with pytest.raises(Exception):
        fetch_org_data(codes, years, add_bid=1)
    with pytest.raises(Exception):
        fetch_org_data(codes, years, add_bid="test")
    with pytest.raises(Exception):
        fetch_org_data(codes, years, add_bid=None)
    with pytest.raises(Exception):
        fetch_org_data(codes, years, language=1)
    with pytest.raises(Exception):
        fetch_org_data(codes, years, language="test")
    with pytest.raises(Exception):
        fetch_org_data(codes, years, language=None)


def test_enrich_data():
    df = __create_dummy_data()
    # Check that data is added correctly
    # Check tat sum is calculated correctly
    # Check that arguments work


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
