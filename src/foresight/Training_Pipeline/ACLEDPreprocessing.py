import datetime

import pandas as pd
import os
from sklearn.linear_model import LinearRegression
import numpy as np

pd.options.display.expand_frame_repr = False
pd.options.display.max_rows = 100

humdata_path = os.path.normpath(os.environ['humdata_path'])

ACLED = pd.read_excel(humdata_path, sheet_name='Data')

month_num = { 'January'  :   1
             ,'February' :   2
             ,'March'    :   3
             ,'April'    :   4
             ,'May'      :   5
             ,'June'     :   6
             ,'July'     :   7
             ,'August'   :   8
             ,'September':   9
             ,'October'  :   10
             ,'November' :   11
             ,'December' :   12 }

ACLED['Month_Num'] = ACLED['Month'].map(month_num)

ACLED['Date'] = pd.to_datetime(ACLED['Month'] + ' ' + ACLED['Year'].astype(str))

class LabelMaker():
    def __init__(self, n_months):
        self.df = None
        self.n_months = n_months
    def get_months(self, start_month, year):
        n_months = self.n_months
        if type(start_month) == str:
            start_month = month_num[start_month]

        month_years = [(year, start_month)]
        month = start_month

        for i in range(n_months):
            if month <= 1:
                month = 12
                year = year - 1
            else:
                month = month - 1

            month_years.append((year, month))

        start_date = month_years[0]
        end_date = month_years[-1]

        date_range = (
            datetime.datetime(start_date[0], start_date[1], 1)
            , datetime.datetime(end_date[0], end_date[1], 1)
        )

        self.date_range = date_range
        return date_range

    def create_base_df(self, country, *args, **kwargs):
        """
        Creates a base df of n months from the start date for a given country
        This can be used for further trend analysis
        """
        daterange = self.get_months(*args, **kwargs)
        df = ACLED[
            (ACLED['Country'].values == country)
            & (ACLED['Date'].values >= np.datetime64(daterange[1]))
            & (ACLED['Date'].values <= np.datetime64(daterange[0]))
            ].reset_index()
        self.base_df = df

    def fatalities_previous_month(self):
        """
        Returns Fatalities for preceding month
        """
        if len(self.base_df) <= 1:
            return None
        return self.base_df['Fatalities'].iloc[-2]

    def n_month_mean(self):
        """
        returns average for the n months preceding the current month
        Does not include current month
        """

        return self.base_df['Fatalities'].iloc[0:-1].mean()

    def n_month_std(self):
        """
        returns average for the n months preceding the current month
        Does not include current month
        """

        return self.base_df['Fatalities'].iloc[0:-1].std()
    def n_month_trend(self):
        """
        Calculates trend for the last n months
        Does include current month
        """

        y = self.base_df.iloc[-n_months:]['Fatalities']
        x = np.array(range(len(y))).reshape(-1, 1)

        model = LinearRegression()
        model.fit(x, y)
        slope = model.coef_
        return slope[0]

    def apply_by_row(self, row):
        self.create_base_df(row['Country'], row['Month'], row['Year'])
        prev = self.fatalities_previous_month()
        mean = self.n_month_mean()
        std = self.n_month_std()
        trend = self.n_month_trend()

        return(prev, mean, std, trend)

"""
START ADDING STUFF
"""

n_months = 3

Labler = LabelMaker(n_months=3)

ACLED = pd.read_excel(humdata_path, sheet_name='Data')

ACLED['Month_Num'] = ACLED['Month'].map(month_num)

ACLED['Date'] = pd.to_datetime(ACLED['Month'] + ' ' + ACLED['Year'].astype(str))

#Scalers
ACLED[['Fatalities_Last_Month',
       f'Mean_Last_{n_months}_Months',
       f'Last_{n_months}_Month_STD',
       'Trend']] = ACLED.apply(Labler.apply_by_row, result_type='expand', axis = 1)

ACLED['Theta'] = np.arctan(ACLED['Trend'])
ACLED['Deviation_From_Mean'] = ACLED['Fatalities'] - ACLED[f'Mean_Last_{n_months}_Months']

#Bools
ACLED['Fatalities_Bool'] = ACLED['Fatalities']>0
ACLED['More_Than_Last_month'] = ACLED['Fatalities']>ACLED['Fatalities_Last_Month']
ACLED['More_Than_Average'] = ACLED['Fatalities']>ACLED[f'Mean_Last_{n_months}_Months']
ACLED[f'Outside_{n_months}_Month_Std'] = ACLED['Deviation_From_Mean'] > ACLED[ f'Last_{n_months}_Month_STD']
ACLED['Trend_Increasing'] = ACLED['Trend']>0

