# Databricks notebook source
import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

def goal_counter(df):

    # Groupyby Team to get Total Goals Home and Away
    home_team_goal_count = df.groupby('home_team_name')['home_team_goal_count'].sum()
    away_team_goal_count = df.groupby('away_team_name')['away_team_goal_count'].sum()

    # Merge Home and Away Goals
    team_goal_count = pd.merge(home_team_goal_count, away_team_goal_count, left_index=True, right_index=True)

    return team_goal_count

df_goal = goal_counter(df)

# Reset Index to get Team back as Column
df_goal.reset_index(inplace=True)

# Rename Columns
df_goal = df_goal.rename(columns=({'home_team_name': 'team_name', 'home_team_goal_count': 'home_goals', 'away_team_goal_count': 'away_goals'}))

# Create Total Goals Column
df_goal['total_goals'] = df_goal['home_goals'] + df_goal['away_goals']

# Create Team IDs
# df_goal['Team ID'] = pd.factorize(df_goal['Team Name'][0])
df_goal['team_ID'] = df_goal.index + 1

# Changing Column Order
df_goal = df_goal.reindex(columns=['team_ID', 'team_name', 'home_goals', 'away_goals', 'total_goals'])

dim_goals = df_goal

dim_goals_spark = spark.createDataFrame(dim_goals)
dim_goals_spark.write.format('delta').mode('overwrite').saveAsTable('mart.dim_goals')
