# Databricks notebook source
import pandas as pd

# COMMAND ----------

spark_df = spark.read.format('delta').table('cleansed.england_premier_league_matches_2018_to_2019_stats_3_csv')
df = spark_df.toPandas()
df

# COMMAND ----------

# Result or WInner / Draw

def determine_result(row):

    if row['home_team_goal_count'] > row['away_team_goal_count']:
        return row['home_team_name']
    
    elif row['home_team_goal_count'] < row['away_team_goal_count']:
        return row['away_team_name']
    
    else:
        return 'Draw'

# Apply the function row-wise to the DataFrame
df['winner'] = df.apply(determine_result, axis=1)
df

# COMMAND ----------

df['home_team_name'].unique()

team_names = [

        'Manchester United', 'Newcastle United', 'AFC Bournemouth',
        'Fulham', 'Huddersfield Town', 'Watford',
        'Wolverhampton Wanderers', 'Liverpool', 'Southampton', 'Arsenal',
        'Cardiff City', 'Everton', 'Leicester City', 'Tottenham Hotspur',
        'West Ham United', 'Chelsea', 'Burnley', 'Manchester City',
        'Brighton & Hove Albion', 'Crystal Palace'
        
       ]

# COMMAND ----------

# 1st Half Goals vs Full Time - Conceeded / Scored

df['home_team_second_half_goals'] = df['total_goal_count'] - df['home_team_goal_count_half_time']
df['away_team_second_half_goals'] = df['total_goal_count'] - df['away_team_goal_count_half_time']

def goal_proficiency_by_half(row, venue):

    first_half_goal_count = f'{venue}_team_goal_count_half_time'
    second_half_goal_count = f'{venue}_team_second_half_goals'

    if row[first_half_goal_count] > row[second_half_goal_count]:
        return 'More Goals in First Half'
    
    if row[first_half_goal_count] < row[second_half_goal_count]:
        return 'More Goals in Second Half'
    
    if row[first_half_goal_count] == row[second_half_goal_count]:
        return "Equal Halves"

df['home_goals_proficiency_by_half'] = df.apply(lambda row: goal_proficiency_by_half(row, 'home'), axis=1)
df['away_goals_proficiency_by_half'] = df.apply(lambda row: goal_proficiency_by_half(row, 'away'), axis=1)
df


# COMMAND ----------

            # Columns to Create
        
#     CREATE GROUP (Time when most goals scored)

# COMMAND ----------

# Which team has the most and least cards over a season?

def team_with_most_cards(df):

    # Create new columns for total yellow and red cards for home and away teams
    df['home_team_total_cards'] = df['home_team_yellow_cards'] + df['home_team_red_cards']
    df['away_team_total_cards'] = df['away_team_yellow_cards'] + df['away_team_red_cards']

    # Aggregate total cards by home and away teams
    home_cards = df.groupby('home_team_name')['home_team_total_cards'].sum()
    away_cards = df.groupby('away_team_name')['away_team_total_cards'].sum()

    # Combine home and away cards into one series
    total_cards = home_cards.append(away_cards)

    # Find the team with the most and least cards
    max_cards_team = total_cards.idxmax()
    max_cards_value = total_cards.max()

    min_cards_team = total_cards.idxmin()
    min_cards_value = total_cards.min()

    return max_cards_team, max_cards_value,min_cards_team, min_cards_value


# Call the function and get results
max_cards_team, max_cards_value, min_cards_team, min_cards_value = team_with_most_cards(df)

# Print the team with the most and least cards
print(f"The team with the most cards is {max_cards_team} with {max_cards_value} cards.")
print(f"The team with the least cards is {min_cards_team} with {min_cards_value} cards.")

# COMMAND ----------

#  dim_matches
#  Self Join
#  Perform a self-merge on game week and team names

# COMMAND ----------

        # Goals

# -- Which team scores the most total goals?
# -- Which team scores the most home / away goals?

# At what time in the season is xG the highest across the league?
# Is there a time period where certain teams are more proflific (after transfers)?
# Who scores the most in the first / second half?
# Who conceeds the most goals before half time / EOT?
# What time period by 5min windows are most goals scored?
# How clinical? Shots on target vs goals
# Posession vs Result

# COMMAND ----------

    # Attendance and 13th Man

# Which team has the highest average attendance?
# Which team has the max attendance and in which game?
# Which rivalry gets the most above average attendance? E.g. avg = 20,000 but rival = 40,000
# Which ground is the biggest fortress?
# Which ground has the worst goals conceeded record?

# COMMAND ----------

# Build Inidvidual Team Profiles of Statistics for Each Game Week
