# Databricks notebook source
import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

# Whiich referee gives out the most Red / Yellow / Total Cards?

# Most Total Cards in one Game
df['total_cards'] = df['home_team_yellow_cards'] + df['home_team_red_cards'] + df['away_team_yellow_cards'] + df['away_team_red_cards']

# Total Yellow Cards
df['total_yellow_cards'] = df['home_team_yellow_cards'] + df['away_team_yellow_cards']

# Total Red Cards
df['total_red_cards'] = df['home_team_red_cards'] + df['away_team_red_cards']

# Gather Summary Statistics by Referee
total_cards_series = df.groupby(['referee', 'game_week'])['total_cards'].sum().sort_values()
total_yellow_series = df.groupby(['referee', 'game_week'])['total_yellow_cards'].sum().sort_values()
total_red_series = df.groupby(['referee', 'game_week'])['total_red_cards'].sum().sort_values()


df_cards = pd.concat([total_cards_series, total_yellow_series, total_red_series], axis = 1)
df_cards.reset_index(inplace=True)

df_cards['gw_ref_cards_id'] = df_cards.index + 1
df_cards = df_cards.reindex(columns=['gw_ref_cards_id', 'referee', 'game_week', 'total_cards', 'total_yellow_cards', 'total_red_cards'])

dim_referee = df_cards

# Load to DBFS
spark_df = spark.createDataFrame(dim_referee)
spark_df.write.format('delta').mode('overwrite').saveAsTable('mart.dim_referee')

