#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd


# Get the labels of values
file = "/home/tvborm/Downloads/taulukkomallit_30062020/taulukkomalli-KKTR_2019-v10-kktr_gaap_ind.xlsm"
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
df_lab = pd.DataFrame()
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[4:, [1, 3]]
    fields = fields.dropna(how="any")
    fields.columns = ["lab", "value"]
    fields["raportointikokonaisuus"] = "KKTR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
# Get the labels of values
file = "/home/tvborm/Downloads/taulukkomallit_30062020/taulukkomalli-KKNR_2020-v11-kknr_gaap_ind.xlsm"
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[:, 1:]
    fields = fields.dropna(subset=fields.columns[0], how="any")
    fields = pd.melt(fields, id_vars=[fields.columns[0]], value_name="value")
    fields = fields.rename(columns={"Unnamed: 1": "lab"})
    fields["raportointikokonaisuus"] = "KKNR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
# Get the labels of values
file = "/home/tvborm/Downloads/taulukkomallit_30062020/taulukkomalli-KKOTR_2019-v10-kkotr_gaap_con.xlsm"
sheets = pd.ExcelFile(file).sheet_names
sheets = [x for x in sheets if x not in ["Sisällys", "t00", "Setup"]]
for i in sheets:
    fields = pd.read_excel(file, sheet_name=i)
    fields = fields.iloc[4:, [1, 3]]
    fields = fields.dropna(how="any")
    fields.columns = ["lab", "value"]
    fields["raportointikokonaisuus"] = "KKOTR"
    df_lab = pd.concat([df_lab, fields], axis=0, ignore_index=True)
df_lab = df_lab.loc[:, ["lab", "value", "raportointikokonaisuus"]]

path = "../src/osta/resources/financial_codes.csv"
df_lab.to_csv(path)
