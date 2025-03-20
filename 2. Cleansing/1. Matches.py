# Databricks notebook source
import pandas as pd
import numpy as np
from pyspark.sql.functions import col

# COMMAND ----------

spark_df = spark.read.format("delta").table("default.england_premier_league_matches_2018_to_2019_stats_3_csv")
df = spark_df.toPandas()
df

# COMMAND ----------

# RUN ONCE - Correcting Column Names

column_names = df.iloc[0]
df.columns = column_names
df = df[1:].reset_index(drop=True)
df

# COMMAND ----------

# Total NaN Values

df.isnull().sum().sum()

# COMMAND ----------

# Columns with null values

non_null_counts = df.count()
columns_with_few_non_nulls = non_null_counts[non_null_counts < 380].index
columns_with_few_non_nulls

# COMMAND ----------

# Convert Columns with Missing Values to Array (Goal Time)

to_array_columns = df.columns[df.isnull().any()]

def convert_to_array(df, columns):
    for column in columns:
            df[column] = df[column].str.split(',').apply(lambda x: x if x is not None else [])
    return df
    
df = convert_to_array(df, to_array_columns)

df.head()

# COMMAND ----------


# Split Timestamp into Year, Month, Week, Day, Hour to GroupBY these metrics for insights
df['date_GMT'] = pd.to_datetime(df['date_GMT'], format='%b %d %Y - %I:%M%p')

# Separate Time Metrics into Separate Columns 
df['year'] = df['date_GMT'].dt.year
df['month'] = df['date_GMT'].dt.month
df['week'] = df['date_GMT'].dt.week
df['day'] = df['date_GMT'].dt.day
df['hour'] = df['date_GMT'].dt.hour
df.head()

# COMMAND ----------

# RUN ONCE

# Separate City Name into Separate Column from Stadium Name

df['location'] = df['stadium_name'].str.extract(r'\((.*?)\)')
df['stadium_name'] = df['stadium_name'].str.replace(r'\s*\(.*?\)', '', regex=True)
df.head() 

# COMMAND ----------

new_columns = [

    'date_GMT','attendance','home_team_name','away_team_name','referee', 'Game Week', 'home_team_goal_count', 'away_team_goal_count', 'total_goal_count', 'total_goals_at_half_time', 'home_team_goal_count_half_time', 'away_team_goal_count_half_time', 'home_team_goal_timings', 'away_team_goal_timings', 'home_team_yellow_cards', 'home_team_red_cards', 'away_team_yellow_cards', 'away_team_red_cards', 'home_team_first_half_cards', 'home_team_second_half_cards', 'away_team_first_half_cards', 'away_team_second_half_cards', 'home_team_shots', 'away_team_shots', 'home_team_shots_on_target', 'away_team_shots_on_target', 'home_team_shots_off_target', 'away_team_shots_off_target', 'home_team_fouls', 'away_team_fouls', 'home_team_possession', 'away_team_possession', 'stadium_name', 'location', 'year', 'month', 'week', 'day', 'hour'
    ]

df_trim = df[new_columns]
df_trim.head()

# COMMAND ----------

# Amend Data Types and Load

df_trim = df_trim.astype({
    'attendance': 'int64', 'Game Week': 'int64', 'home_team_goal_count': 'int64', 'away_team_goal_count': 'int64', 'total_goal_count': 'int64', 'total_goals_at_half_time': 'int64', 'home_team_goal_count_half_time': 'int64', 'away_team_goal_count_half_time': 'int64', 'home_team_yellow_cards': 'int64', 'home_team_red_cards': 'int64', 'away_team_yellow_cards': 'int64', 'away_team_red_cards': 'int64', 'home_team_first_half_cards': 'int64', 'home_team_second_half_cards': 'int64', 'away_team_first_half_cards': 'int64', 'away_team_second_half_cards': 'int64', 'home_team_shots': 'int64', 'away_team_shots': 'int64', 'home_team_shots_on_target': 'int64', 'away_team_shots_on_target': 'int64', 'home_team_shots_off_target': 'int64', 'away_team_shots_off_target': 'int64', 'home_team_fouls': 'int64', 'away_team_fouls': 'int64', 'home_team_possession': 'int64', 'away_team_possession': 'int64', 'year': 'int64', 'month': 'int64', 'week': 'int64', 'day': 'int64', 'hour': 'int64', 'stadium_name': 'string', 'location': 'string', 'home_team_name': 'string', 'away_team_name': 'string', 'referee': 'string'})

df_trim.info()


# COMMAND ----------

# Splitting "'" from Timings

df_trim['home_team_goal_timings'] = df_trim['home_team_goal_timings'].apply(
    
    lambda timings: [int(str(t).split("'")[0]) if isinstance(t, str) and "'" in t else int(t) for t in timings])

df_trim['away_team_goal_timings'] = df_trim['away_team_goal_timings'].apply(
    
    lambda timings: [int(str(t).split("'")[0]) if isinstance(t, str) and "'" in t else int(t) for t in timings])

df_trim.head()

# COMMAND ----------

# DO NOT RUN - INCORRECT

# FIND A SLICING SOLUTION INSTED OF COMPLEX LAMBDA ABOVE

# def normalize_goal_times(df, column):

#     for index, value in df[column].items():

#             string_value = str(value)

#             if len(string_value) > 2: 
                
#                 df.at[index, column] = string_value[:2]

#     return df

# normalize_goal_times(df, 'home_team_goal_timings')


# COMMAND ----------

# Final Column Name Clean

df_trim.columns = df_trim.columns.str.replace(r'[ ,;{}()\n\t=]', '_', regex=True).str.lower()
df_trim

# COMMAND ----------

spark_df = spark.createDataFrame(df_trim)

spark_df.write.format('delta').mode('overwrite').saveAsTable('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv
