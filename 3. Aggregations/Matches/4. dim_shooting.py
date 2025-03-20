# Databricks notebook source
# Having issue with Pivoting Home and Away Team on Separate Rows

# COMMAND ----------

import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

# Shot Conversion

def home_shot_conversion(row, venue):

    team_goal_count = f'{venue}_team_goal_count'
    team_shots = f'{venue}_team_shots'

    if row[team_goal_count] == 0:
        return round(0, 2)
    return round(row[team_goal_count] / row[team_shots], 2)

df['home_shot_conversion'] = df.apply(lambda row: home_shot_conversion(row, 'home'), axis=1)
df['away_shot_conversion'] = df.apply(lambda row: home_shot_conversion(row, 'away'), axis=1)

# Shot Accuracy

def shot_accuracy(row, venue):

    shots_on_target = f"{venue}_team_shots_on_target"
    total_shots = f"{venue}_team_shots"
    
    if row[shots_on_target] == 0:
        return round(0, 2)
    return round(row[shots_on_target] / row[total_shots], 2)


df['home_shot_accuracy'] = df.apply(lambda row: shot_accuracy(row, 'home'), axis = 1)
df['away_shot_accuracy'] = df.apply(lambda row: shot_accuracy(row, 'away'), axis = 1)

df[['home_team_name', 'away_team_name', 'game_week', 'home_shot_accuracy', 'away_shot_accuracy', 'home_shot_conversion', 'away_shot_conversion']].head()
