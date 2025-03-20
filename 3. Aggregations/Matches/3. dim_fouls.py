# Databricks notebook source
import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

# Team with the most and least fouls

def biggest_foulers(df):

    # Groupby Team Name to get Totals by Home and Away
    home_team_foulers =df.groupby('home_team_name')['home_team_fouls'].sum()
    away_team_foulers =df.groupby('away_team_name')['away_team_fouls'].sum()

    # Join on Index to get Home Total and Away Total 
    team_foulers = pd.merge(home_team_foulers, away_team_foulers, left_index= True, right_index= True, how='inner')

    # Max and Min Team Names
    max_foulers_team = team_foulers.idxmax()
    min_foulers_team = team_foulers.idxmin()

    # Max and Min Team Values
    max_team_foulers_value = team_foulers.max()
    min_team_foulers_value = team_foulers.min()

    return max_foulers_team, min_foulers_team, max_team_foulers_value, min_team_foulers_value, team_foulers

max_foulers_team, min_foulers_team, max_team_foulers_value, min_team_foulers_value, team_foulers = biggest_foulers(df)


# Reset Index to get Team Name as Column
team_foulers = team_foulers.reset_index()

team_foulers = team_foulers.rename(columns=({'home_team_name':'team_name', 'home_team_fouls': 'fouls_at_home', 'away_team_fouls': 'fouls_away'}))

team_foulers['total_fouls'] = team_foulers['fouls_at_home'] + team_foulers['fouls_away']

# Create Foul ID
teams_by_fouls_leaderboard = team_foulers.sort_values(by = 'total_fouls', ascending=False)
teams_by_fouls_leaderboard['foul_ID'] = teams_by_fouls_leaderboard.index + 1

# Reindex Coumns
teams_by_fouls_leaderboard = teams_by_fouls_leaderboard.reindex(columns=['foul_ID', 'team_name', 'fouls_at_home', 'fouls_away', 'total_fouls'])
dim_fouls = teams_by_fouls_leaderboard

# Leaderboard to DataFrame and Save
spark_dim_fouls = spark.createDataFrame(dim_fouls)
spark_dim_fouls.write.format('delta').mode('overwrite').saveAsTable('mart.dim_fouls')
