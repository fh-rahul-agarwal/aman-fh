import pandas as pd
from processing.pillars.activity.dataStream.a_flightsClimbed import *

class AFlightsClimbedAgg:
    def __init__(self, records_df, *args):
        self.records_df = records_df
        self.processor = AFlightsClimbed(self.records_df, *args)
        self.flights_climbed_df = self.processor.process()
        self.type = self.flights_climbed_df['type'].iloc[0]
        self.valueType = 'TotalFlightsClimbed'
        self.s_name = 'A_FlightsClimbed'

    def process(self):
        # Check for duplicate columns and rows
        self.flights_climbed_df = self.flights_climbed_df.loc[:, ~self.flights_climbed_df.columns.duplicated()]
        self.flights_climbed_df = self.flights_climbed_df.drop_duplicates()

        # Convert 'value' column to numeric and date columns to datetime
        self.flights_climbed_df['value'] = pd.to_numeric(self.flights_climbed_df['value'], errors='coerce')
        self.flights_climbed_df['startDate'] = pd.to_datetime(self.flights_climbed_df['startDate'], errors='coerce').dt.date
        self.flights_climbed_df['endDate'] = pd.to_datetime(self.flights_climbed_df['endDate'], errors='coerce').dt.date

        # Aggregate data by userName, date, startDate, endDate, and unit
        self.flights_climbed_df['date'] = self.flights_climbed_df['startDate']
        self.flights_climbed_df = self.flights_climbed_df.groupby(['userName', 'date', 'startDate', 'endDate', 'unit']).agg({'value': 'sum'}).reset_index()

        # Keep rows with the maximum value for each date
        self.flights_climbed_df = self.flights_climbed_df.loc[self.flights_climbed_df.groupby('date')['value'].idxmax()]

        # Add additional columns and metadata
        self.flights_climbed_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.flights_climbed_df['value'] = self.flights_climbed_df['value'].round(1)
        self.flights_climbed_df['type'] = self.type
        self.flights_climbed_df['valueType'] = self.valueType
        self.flights_climbed_df['s_name'] = self.s_name

        # Reorder columns and sort by date
        self.flights_climbed_df = self.flights_climbed_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
        self.flights_climbed_df = self.flights_climbed_df.sort_values(by=['date'], ascending=False).reset_index(drop=True)

        return self.flights_climbed_df
