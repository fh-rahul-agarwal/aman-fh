import pandas as pd
from datetime import datetime

from processing.pillars.activity.dataStream.a_activityCalories import *

class AActivityCaloriesAgg:

    def __init__(self, records_df, workouts_df, *args, classProcessorName = AActivityCalories):
        self.records_df = records_df
        self.workouts_df = workouts_df
        self.args = args
        self.user_name = self.records_df['userName'].iloc[0] if 'userName' in self.records_df.columns else 'UnknownUser'
        self.processor_class = classProcessorName
        self.processor = self.processor_class(self.records_df, self.workouts_df, *self.args)
        self.processed_df = self.processor.process()
        self.type = self.processed_df['type'].iloc[0]
        self.s_name = 'A_ActivityCalories'

    def process(self):
        processed_df = self.processed_df

        # Extract 'type' and 'unit' from the processor class's output
        if not processed_df.empty:
            type_value = processed_df['type'].iloc[0] if 'type' in processed_df.columns else None
            unit_value = processed_df['unit'].iloc[0] if 'unit' in processed_df.columns else None
        else:
            type_value = None
            unit_value = None

        # Ensure 'startDate' is properly handled
        processed_df['startDate'] = pd.to_datetime(processed_df['startDate'], errors='coerce')
        processed_df = processed_df.dropna(subset=['startDate'])

        # Convert 'value' column to numeric, coercing errors to NaN
        processed_df['value'] = pd.to_numeric(processed_df['value'], errors='coerce')

        # Drop duplicates based on 'startDate'
        processed_df = processed_df.drop_duplicates(subset='startDate')

        # Check if 'activity', 'sleep', 'workout', 'resting' columns exist and set to 0 if not present
        for col in ['activity', 'sleep', 'workout', 'resting']:
            if col not in processed_df.columns:
                processed_df[col] = 0

        # Group by 'startDate' and calculate Sum for each segment
        daily_agg = processed_df.groupby(processed_df['startDate'].dt.date).agg(
            FlightsClimbedActiveCaloriesBurned=('value', lambda x: x[processed_df.loc[x.index, 'flightsClimbed'] == 1].sum() if (processed_df.loc[x.index, 'flightsClimbed'] == 1).any() else pd.NA),
            StepCountActiveCaloriesBurned=('value', lambda x: x[processed_df.loc[x.index, 'stepCount'] == 1].sum() if (processed_df.loc[x.index, 'stepCount'] == 1).any() else pd.NA),
            StandTimeActiveCaloriesBurned=('value', lambda x: x[processed_df.loc[x.index, 'standTime'] == 1].sum() if (processed_df.loc[x.index, 'standTime'] == 1).any() else pd.NA),
            OtherActivitiesActiveCaloriesBurned=('value', lambda x: x[processed_df.loc[x.index, 'otherActivities'] == 1].sum() if (processed_df.loc[x.index, 'otherActivities'] == 1).any() else pd.NA),
        ).reset_index()

        # Fill NaN values with appropriate defaults
        daily_agg = daily_agg.fillna({'FlightsClimbedActiveCaloriesBurned': pd.NA, 
                                      'StepCountActiveCaloriesBurned': pd.NA, 
                                      'StandTimeActiveCaloriesBurned': pd.NA,
                                      'OtherActivitiesActiveCaloriesBurned': pd.NA})

        # Calculate total basal energy burned
        daily_agg['TotalActivityCaloriesBurned'] = daily_agg[['FlightsClimbedActiveCaloriesBurned', 
                                                         'StepCountActiveCaloriesBurned', 
                                                         'StandTimeActiveCaloriesBurned', 
                                                         'OtherActivitiesActiveCaloriesBurned']].sum(axis=1)

        # Restructure DataFrame to long format
        long_format = daily_agg.melt(id_vars=['startDate'], 
                                     value_vars=['FlightsClimbedActiveCaloriesBurned', 
                                                 'StepCountActiveCaloriesBurned',
                                                 'StandTimeActiveCaloriesBurned',
                                                 'OtherActivitiesActiveCaloriesBurned',
                                                 'TotalActivityCaloriesBurned'],
                                     var_name='valueType', 
                                     value_name='value')

        # Add 'type' and 'unit' columns to the DataFrame
        long_format['type'] = type_value
        long_format['unit'] = unit_value

        # Ensure 'value' column is numeric and handle NaNs
        long_format['value'] = pd.to_numeric(long_format['value'], errors='coerce')

        # Round the 'value' column to 1 decimal place, safely ignoring NaNs
        long_format['value'] = long_format['value'].apply(lambda x: round(x, 1) if pd.notna(x) else x)

        # Reorder rows to place 'totalActivityCaloriesBurned' on top and sort by descending value
        long_format['order'] = long_format['valueType'].apply(lambda x: 0 if x == 'TotalActivityCaloriesBurned' else 1)
        long_format = long_format.sort_values(by=['startDate', 'order', 'value'], ascending=[True, True, False]).drop('order', axis=1)
        long_format['date'] = long_format['startDate']

        # Reorder columns to match the desired output
        final_output = long_format[['date', 'type', 'unit', 'valueType', 'value']]
        final_output = final_output.sort_values(by=['date', 'unit'], ascending=False).reset_index(drop=True)

        final_output['userName'] = self.user_name
        final_output['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_output['type'] = self.type
        final_output['s_name'] = self.s_name

        # Return the restructured DataFrame
        return final_output[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
