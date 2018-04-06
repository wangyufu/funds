#!/usr/bin/env python
import pandas as pd
"""
code_id,feature_manifestation,fund_type,level,manager,quarter_low_ranking,running_time,year_low_ranking,year_manifestation
"""

df = pd.read_csv('Well_behaved_fund.csv', sep=',', encoding="utf-8")
a = df[(df.loc[:, 'quarter_low_ranking'] < 0.5) & (df.loc[:, 'year_low_ranking'] < 0.5) & (df.loc[:, 'fund_type'] == '债券型')]
a.to_csv('Well_behaved_fundzhaijuan.csv', sep=',', encoding="utf-8", index=False)



