# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# ## Reading Parquet datasets with PyArrow and Pandas

import pandas as pd
import pyarrow.parquet as pq

# pandas can read parquet files directly (and can get a subset of columns)
# and by default uses pyarrow under the hood
df = pd.read_parquet('../pd_datasets/gkg', columns=['V2Locations', 'docembed'])

# pyarrow has some additional, useful functionality
# pyarrow.parquet.ParquetDataset can read metadata without loading data into RAM
# https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html#pyarrow.parquet.ParquetDataset
ds = pq.ParquetDataset('../pd_datasets/gkg', use_legacy_dataset=False)

ds.files

ds.schema

# can use filters to load only certain rows conditionally
ds = pq.ParquetDataset('../pd_datasets/gdelt', use_legacy_dataset=False, filters=[('lang', '=', 'ENGLISH')])

# read specific subset of columns from the dataset into memory as a pyarrow.Table
# (read_pandas means it will read any pandas-specific metatdata that
#  was included when writing the parquet file)
# NOTE that the Table API has some useful stuff that will be
# more memory efficient than pandas
# https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table
table = ds.read_pandas(columns=['V2Locations', 'docembed'])

table.shape

table.schema

# convert to pandas DataFrame
df = table.to_pandas()

# ## Extracting country codes from V2Locations

import re

# regex to find iso2 country code within a location starting with 1
re.search(r'1#\w+#(?P<country>\w{2})#', '1#Taiwan#TW#TW##24#121#TW#5874;').group('country')

# example V2Locations for a single record
txt = """
2#Iowa, United States#US#USIA##42.0046#-93.214#IA#6714;4#Wuhan, Hubei, China#CH#CH12#13124#30.5833#114.267#-1930776#726;1#Taiwan#TW#TW##24#121#TW#5774;1#Taiwan#TW#TW##24#121#TW#5874;3#University Of Nebraska Medical Center, Nebraska, United States#US#USNE#NE055#41.255#-95.9767#834298#3513;1#Americans#US#US##39.828175#-98.5795#US#3318;2#California, United States#US#USCA##36.17#-119.746#CA#2950;2#California, United States#US#USCA##36.17#-119.746#CA#3024;2#California, United States#US#USCA##36.17#-119.746#CA#3377;1#Chinese#CH#CH##35#105#CH#316;1#Chinese#CH#CH##35#105#CH#1921;1#Chinese#CH#CH##35#105#CH#1974;1#Chinese#CH#CH##35#105#CH#5362;3#Travis Air Force Base, California, United States#US#USCA#CA095#38.2685#-121.934#2512359#3103;4#Haneda, Wakayama, Japan#JA#JA43#36528#33.75#135.333#-228452#4354;4#London, London, City Of, United Kingdom#UK#UKH9#40110#51.5#-0.116667#-2601889#6738;2#Texas, United States#US#USTX##31.106#-97.6475#TX#3032;2#Texas, United States#US#USTX##31.106#-97.6475#TX#3283;3#Des Moines, Iowa, United States#US#USIA#IA153#41.6005#-93.6091#465961#6692;4#Beijing, Beijing, China#CH#CH22#13001#39.9289#116.388#-1898541#6658;4#Hubei, Guangdong, China#CH#CH30#13047#23.2656#116.054#10073728#762;4#Hubei, Guangdong, China#CH#CH30#13047#23.2656#116.054#10073728#1763;3#Lackland Air Force Base, Texas, United States#US#USTX#TX029#29.3878#-98.6064#2512527#3259;3#Lackland Air Force Base, Texas, United States#US#USTX#TX029#29.3878#-98.6064#2512527#4942;1#Japanese#JA#JA##36#138#JA#2423;1#Japanese#JA#JA##36#138#JA#4439;1#Japanese#JA#JA##36#138#JA#6068;1#Japanese#JA#JA##36#138#JA#6347;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#2961;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3454;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3498;2#Nebraska, United States#US#USNE##41.1289#-98.2883#NE#3595;1#Japan#JA#JA##36#138#JA#4361;1#Japan#JA#JA##36#138#JA#6514;1#American#US#US##39.828175#-98.5795#US#2818;1#American#US#US##39.828175#-98.5795#US#4278;3#San Francisco, California, United States#US#USCA#CA075#37.7749#-122.419#277593#3139;3#San Francisco, California, United States#US#USCA#CA075#37.7749#-122.419#277593#6791;1#China#CH#CH##35#105#CH#25;1#China#CH#CH##35#105#CH#733;1#China#CH#CH##35#105#CH#756;1#China#CH#CH##35#105#CH#769;1#China#CH#CH##35#105#CH#826;1#China#CH#CH##35#105#CH#906;1#China#CH#CH##35#105#CH#1086;1#China#CH#CH##35#105#CH#1770;1#China#CH#CH##35#105#CH#1805;1#China#CH#CH##35#105#CH#2720;1#China#CH#CH##35#105#CH#5708;1#China#CH#CH##35#105#CH#5764;1#China#CH#CH##35#105#CH#5866;1#China#CH#CH##35#105#CH#6258;1#China#CH#CH##35#105#CH#6665
"""

# TODO confirm significance of the integers at the beginning
# of each location description
# use same regex to find all of them for a given record's V2Locations
re.findall(r'1#\w+#(?P<country>\w{2})#', txt)

# + tags=[]
df.fillna("", inplace=True)
# -

pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', -1)

# get a few records
df.head()['V2Locations']

# use pandas' extractall to apply regex to dataframe
matches = df.head()['V2Locations'].str.extractall(r'1#\w+#(?P<country>\w{2})#')

# this returns a new multi-index dataframe of the results
# that we'll need to reformat in order to add a 'countries'
# column to the original dataframe
matches

# unstack multiindex
_matches = matches.unstack()

# drop one level
_matches.columns = _matches.columns.droplevel()

# now we have one row per record with
# variable numbers of column values
_matches

# create temporary dataframe with unique countries seen in each row
# as a column containing lists while preserving index that 
# references original dataframe indices
_df = pd.DataFrame(enumerate(list(map(set,_matches.values))), index=_matches.index)
_df = _df.rename(columns={1: 'countries'})
# lists contain NaNs since the number of extracted countries varies per record,
# so map a crazy double lambda expression to remove them
_df['countries'] = _df['countries'].map(lambda x: list(filter(lambda y: not pd.isna(y), x)))
_df

# finally, join the new 'countries' column of lists
# to the original dataframe
df = df.head().join(_df['countries'])

df
