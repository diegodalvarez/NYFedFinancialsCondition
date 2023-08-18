# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 12:14:48 2023

@author: Diego Alvarez
@email: diego.alvarez@colorado.edu
"""

# data will overwrite
import pandas as pd

class NYFedCollect:
    
    def __init__(self):

        self.link = "https://www.federalreserve.gov/econres/notes/feds-notes/fci-g_public_monthly-3281.csv"
        self.storage_options = {"User-Agent": "Mozilla/5.0"}
        
        self.column_renamer = {
            "FCI-G Index (baseline)": "fci_baseline",
            "FCI-G Index (one-year lookback)": "fci_rolling_year"}
        
        self.inv_column_renamer = {v: k for k, v in self.column_renamer.items()}
        
    def collect(self):
        
        self.df = pd.read_csv(self.link, storage_options = self.storage_options)
        
        print("Collecting information")
        
        (self.df.reset_index().melt(
            id_vars = "date").
            to_parquet(
                path = "financial_conditions_raw.parquet",
                engine = "pyarrow"))
        
        print("Raw Data Saved, Now Prepping Data")
        self.prep_data()
        
        
    def prep_data(self):
        
        df_raw = pd.read_parquet(
            path = "financial_conditions_raw.parquet",
            engine = "pyarrow")
        
        df_raw = (df_raw.pivot(
            index = "date", columns = "variable", values = "value").
            reset_index().
            rename(columns = self.column_renamer).
            drop(columns = ["index"]).
            assign(date = lambda x: pd.to_datetime(x.date)))
        
        self.df_prep = df_raw
        
        (df_raw.to_parquet(
            path = "financial_conditions_prep.parquet",
            engine = "pyarrow"))
        
        print("Data Prepped and saved")
        
    def get_df(self): 
        
        try: 
            return(self.df_prep)
        
        except:
            return(pd.read_parquet("financial_conditions_prep.parquet"))