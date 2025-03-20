# Databricks notebook source
import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('default.england_premier_league_players_2018_to_2019_stats_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

# Defender Stats Dim

df_defenders = df[df['position'] == 'Defender']

defender_stats_dim = df_defenders[['full_name', 'Current Club', 'nationality', 'clean_sheets_overall', 'clean_sheets_home', 'clean_sheets_away', 'conceded_overall', 'conceded_home', 'conceded_away', 'min_per_conceded_overall']]

defender_stats_dim = defender_stats_dim.rename(columns={'Current Club': 'current_club'})

def_stats_dim = defender_stats_dim

spark_df= spark.createDataFrame(def_stats_dim)
spark_df.write.format('delta').mode('overwrite').saveAsTable('cleansed.def_stats_dim')
