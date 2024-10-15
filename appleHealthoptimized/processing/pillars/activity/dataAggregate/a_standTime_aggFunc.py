import pandas as pd
from processing.pillars.activity.dataStream.a_standTime import *

class AStandTimeAgg:
    def __init__(self, records_df, *args):
        self.records_df = records_df
        self.processor = AStandTime(self.records_df, *args)
        self.stand_time_df = self.processor.process()
        self.type = self.stand_time_df['type'].iloc[0]
        self.valueType = 'TotalStandTime'
        self.s_name = 'A_StandTime'

    def process(self):
        # Check for duplicate columns and rows
        self.stand_time_df = self.stand_time_df.loc[:, ~self.stand_time_df.columns.duplicated()]
        self.stand_time_df = self.stand_time_df.drop_duplicates()

        # Convert 'value' column to numeric and date columns to datetime
        self.stand_time_df['value'] = pd.to_numeric(self.stand_time_df['value'], errors='coerce')
        self.stand_time_df['startDate'] = pd.to_datetime(self.stand_time_df['startDate'], errors='coerce').dt.date
        self.stand_time_df['endDate'] = pd.to_datetime(self.stand_time_df['endDate'], errors='coerce').dt.date

        # Set the date column
        self.stand_time_df['date'] = self.stand_time_df['startDate']

        # Aggregate data by userName, date, startDate, endDate, and unit
        self.stand_time_df = self.stand_time_df.groupby(['userName', 'date', 'startDate', 'endDate', 'unit']).agg({'value': 'sum'}).reset_index()

        # Keep rows with the maximum value for each date
        self.stand_time_df = self.stand_time_df.loc[self.stand_time_df.groupby('date')['value'].idxmax()]

        # Add additional columns and metadata
        self.stand_time_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.stand_time_df['value'] = self.stand_time_df['value'].round(1)
        self.stand_time_df['type'] = self.type
        self.stand_time_df['valueType'] = self.valueType
        self.stand_time_df['s_name'] = self.s_name

        # Reorder columns and sort by date
        self.stand_time_df = self.stand_time_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
        self.stand_time_df = self.stand_time_df.sort_values(by=['date'], ascending=False).reset_index(drop=True)

        return self.stand_time_df
