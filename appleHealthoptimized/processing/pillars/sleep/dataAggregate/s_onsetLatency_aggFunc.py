import pandas as pd
from processing.pillars.sleep.dataStream.s_typeSleep import SSleepType

class SOnsetLatencyAgg:
    def __init__(self, records_df: pd.DataFrame, *args):
        self.records_df = records_df
        self.sleep_data_processor = SSleepType(self.records_df, *args)
        self.sleep_df = self.sleep_data_processor.process()
        self.type = self.sleep_df['type'].iloc[0] 
        self.s_name = 'S_OnsetLatency'

    def process(self) -> pd.DataFrame:
        sleep_df = self.sleep_df

        # Convert 'creationDate' and 'endDate' to datetime format
        sleep_df['creationDate'] = pd.to_datetime(sleep_df['creationDate'])
        sleep_df['endDate'] = pd.to_datetime(sleep_df['endDate'])
        sleep_df['startDate'] = pd.to_datetime(sleep_df['startDate'])
        
        # Filter the DataFrame for in-bed and asleep core records
        in_bed_df = sleep_df[sleep_df['value'] == 'HKCategoryValueSleepAnalysisInBed']
        asleep_core_df = sleep_df[sleep_df['value'] == 'HKCategoryValueSleepAnalysisAsleepCore']

        # Initialize a list to store onset latency data
        onset_latency_data = []

        # Iterate through each date in the sleep_df
        for date in sleep_df['creationDate'].dt.date.unique():
            # Filter data for the current date
            daily_in_bed_df = in_bed_df[in_bed_df['creationDate'].dt.date == date]
            daily_asleep_core_df = asleep_core_df[asleep_core_df['creationDate'].dt.date == date]

            # Iterate through each in-bed period for the current date
            for idx, row in daily_in_bed_df.iterrows():
                # Find the first 'AsleepCore' event that occurred after the 'InBed' event
                relevant_asleep_core = daily_asleep_core_df[
                    (daily_asleep_core_df['startDate'] >= row['endDate']) &
                    (daily_asleep_core_df['userName'] == row['userName'])
                ].head(1)

                if not relevant_asleep_core.empty:
                    asleep_start = pd.to_datetime(relevant_asleep_core.iloc[0]['startDate'])
                    in_bed_end = pd.to_datetime(row['endDate'])

                    # Calculate latency in minutes
                    latency_minutes = round((asleep_start - in_bed_end).total_seconds() / 60.0, 1)

                    onset_latency_data.append({
                        'userName': row['userName'],
                        'valueGeneratedAt': row['valueGeneratedAt'],
                        'date': date,
                        'type': self.type,
                        'unit': 'min',
                        'valueType': 'SleepOnsetLatency',
                        'value': latency_minutes,
                    })

                    onset_latency_data.append({
                        'userName': row['userName'],
                        'valueGeneratedAt': row['valueGeneratedAt'],
                        'date': date,
                        'type': self.type,
                        'unit': 'datetime',
                        'valueType': 'InBedStartTime',
                        'value': row['endDate'].strftime('%Y-%m-%d %H:%M:%S'),
                    })

                    onset_latency_data.append({
                        'userName': row['userName'],
                        'valueGeneratedAt': row['valueGeneratedAt'],
                        'date': date,
                        'type': self.type,
                        'unit': 'datetime',
                        'valueType': 'AsleepStartTime',
                        'value': asleep_start.strftime('%Y-%m-%d %H:%M:%S'),
                    })

                    # Break after the first sleep onset latency is found
                    break

        # Convert the list to a DataFrame
        onset_latency_df = pd.DataFrame(onset_latency_data)
        onset_latency_df = onset_latency_df.sort_values(by=['date', 'unit'], ascending=False, ignore_index=True)
        onset_latency_df['s_name'] = self.s_name

        return onset_latency_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
