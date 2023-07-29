# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:14:48 2023

@author: Diego Alvarez
"""

import pandas as pd


link = "https://www.federalreserve.gov/econres/notes/feds-notes/fci-g_public_monthly-3281.csv"
storage_options = {"User-Agent": "Mozilla/5.0"}
        
df = pd.read_csv(link, storage_options = storage_options)

(df.reset_index().melt(
    id_vars = "date").
    to_parquet(
        path = "financial_conditions.parquet",
        engine = "pyarrow"))