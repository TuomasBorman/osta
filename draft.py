#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 10 16:28:48 2023

@author: tvborm
"""

data = {"test1": ["test", "testi", "test"],
        "test2": [1, 2, 3],
        "test3": [True, False, False],
        "org_name": ["Kaarina", "Turku", "test"],
                "tegfg": [1, 2, 3],
                "tefg": [True, False, False]
        }
df = pd.DataFrame(data)

clean_data(df)


__is_non_empty_df(df)
