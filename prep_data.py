# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:17:47 2023

@author: Diego Alvarez
"""

import pandas as pd

df_raw = pd.read_parquet(
    path = "financial_conditions.parquet",
    engine = "pyarrow")

df_raw = (df_raw.pivot(
    index = "date", columns = "variable", values = "value").
    reset_index().
    rename(columns = {
        "FCI-G Index (baseline)": "fci_baseline",
        "FCI-G Index (one-year lookback)": "fci_rolling_year"}).
    drop(columns = ["index"]).
    assign(date = lambda x: pd.to_datetime(x.date)))

(df_raw.to_parquet(
    path = "financial_conditions_prep.parquet",
    engine = "pyarrow"))