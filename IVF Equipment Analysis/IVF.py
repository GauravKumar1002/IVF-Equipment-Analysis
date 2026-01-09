# -*- coding: utf-8 -*-
"""
Created on Wed Dec 31 12:23:33 2025

@author: Gaurav Kumar
"""

import mysql.connector
from sqlalchemy import create_engine
import pandas as pd

engine = create_engine(
    "mysql+mysqlconnector://root:NewPassword123@localhost/ivf_analysis")

query = "SELECT * FROM ivf_equipment"
df = pd.read_sql(query, engine)

df.head()
df.info()
df.shape

df.to_csv("ivf_equipment_from_mysql.csv", index=False)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from ydata_profiling import ProfileReport

df = pd.read_csv("ivf_equipment_from_mysql.csv")

df.head()
df.tail()
df.shape
df.info()

df['date'] = pd.to_datetime(df['date'], errors='coerce')

numeric_cols = [
    'max_capacity_hrs', 'utilization_hrs', 'utilization_pct',
    'idle_hrs', 'technical_downtime_hrs', 'planned_maintenance_hrs',
    'avg_delay_minutes', 'total_cases_day_lab'
]

df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

df.isnull().sum()

df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

df['primary_procedure'] = df['primary_procedure'].fillna('Unknown')
df['redundancy_available'] = df['redundancy_available'].fillna('No')

df.duplicated().sum()
df = df.drop_duplicates()

df[numeric_cols].mean()
df[numeric_cols].median()
df[numeric_cols].var()
df[numeric_cols].std()
df[numeric_cols].skew()
df[numeric_cols].kurtosis()

#Histogram
plt.hist(df['utilization_pct'], bins=20)
plt.xlabel('Utilization Percentage')
plt.ylabel('Frequency')
plt.title('Distribution of Equipment Utilization')
plt.show()

#Boxplot
plt.boxplot(df['idle_hrs'])
plt.title('Idle Hours Distribution')
plt.ylabel('Hours')
plt.show()

#Line Plot(Trend Over Time)
daily_util = df.groupby('date')['utilization_hrs'].mean()
plt.plot(daily_util.index, daily_util.values)
plt.xlabel('Date')
plt.ylabel('Avg Utilization Hours')
plt.title('Utilization Trend Over Time')
plt.show()

#Bar Chart
lab_cases = df.groupby('lab_id')['total_cases_day_lab'].mean()
plt.bar(lab_cases.index, lab_cases.values)
plt.xlabel('Lab ID')
plt.ylabel('Avg Daily Cases')
plt.title('Lab-wise Average Daily Cases')
plt.show()

#Outlier Detection(EDA)
Q1 = df[numeric_cols].quantile(0.25)
Q3 = df[numeric_cols].quantile(0.75)
IQR = Q3 - Q1

outliers = ((df[numeric_cols] < (Q1 - 1.5 * IQR)) | 
            (df[numeric_cols] > (Q3 + 1.5 * IQR))).sum()
outliers

#OUTLIER TREATMENT
df[numeric_cols] = df[numeric_cols].clip(
    lower=Q1 - 1.5 * IQR,
    upper=Q3 + 1.5 * IQR,
    axis=1
)

#FEATURE ENGINEERING (PREPROCESSING)
df['utilization_ratio'] = df['utilization_hrs'] / df['max_capacity_hrs']
df['downtime_total'] = (
    df['technical_downtime_hrs'] + df['planned_maintenance_hrs']
)

profile = ProfileReport(df, title="IVF Equipment AutoEDA", explorative=True)
profile.to_file("IVF_AutoEDA_Report.html")

df.to_csv("ivf_equipment_final_cleaned.csv", index=False)

df.info()
df.describe()

import os
os.listdir()

