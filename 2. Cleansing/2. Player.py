# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

spark_df = spark.read.format('delta').table('default.england_premier_league_players_2018_to_2019_stats_csv')
df = spark_df.toPandas()
df.head()

# COMMAND ----------

# RUN ONCE

# Proper Column Names

column_names = df.iloc[0]
df.columns = column_names
df = df[1:].reset_index(drop=True)
df.head()

# COMMAND ----------

columns = df.columns.str.replace(' ', '_').str.lower()
df.columns = columns
df.head()

# COMMAND ----------

# Columns with null values

non_null_counts = df.count()
columns_with_few_non_nulls = non_null_counts[non_null_counts < non_null_counts.max()].index
columns_with_few_non_nulls

# COMMAND ----------

def correcting_dtypes(df):

    for col in df.columns:
        
        try:
            df[col] = pd.to_numeric(df[col])

            if df[col].apply(lambda x: x % 1 == 0).all():
                df[col].astype(int)

            else:
                df[col].astype(float)

        except Exception as e:
            print(f"Can't convert '{col}' to numeric data type")

    return df

df_type = correcting_dtypes(df)
df_type.info()

# COMMAND ----------

# AGG ACTIVITY
# Defender Stats Dim

df_defenders = df[df['position'] == 'Defender']

defender_stats_dim = df_defenders[['full_name', 'Current Club', 'nationality', 'clean_sheets_overall', 'clean_sheets_home', 'clean_sheets_away', 'conceded_overall', 'conceded_home', 'conceded_away', 'min_per_conceded_overall']]

defender_stats_dim = defender_stats_dim.rename(columns={'Current Club': 'current_club'})

def_stats_dim = defender_stats_dim

spark_df= spark.createDataFrame(def_stats_dim)
spark_df.write.format('delta').mode('overwrite').saveAsTable('cleansed.def_stats_dim')

# COMMAND ----------

# Consider: Minutes Played / Appearances

# Goalkeeper - 

#   clean_sheets_overall, clean_sheets_home, clean_sheets_away, conceded_overall, conceded_home, conceded_away, 
#   //min_per_conceeded//, appearances_per_clean_sheet

# Midfielder = Stats

# Forward - Stats

# Discipline - yellow_cards_overall	red_cards_overall



# Top 10% Ranked Players over 30

