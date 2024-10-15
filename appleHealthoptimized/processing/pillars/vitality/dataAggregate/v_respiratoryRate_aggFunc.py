
import pandas as pd
from datetime import datetime
from processing.typeSegmentation.respiratoryRate_types import RespiratoryRateTypeDivisionProcessor
# import sys
# import importlib.util
# import os

# # Specify the path to the directory containing the module
# # For example, if 'typeSegmentation' is in 'C:\Users\amank\Downloads\Fithack\Optimization\processing'
# module_path = os.path.abspath(r'C:\Users\amank\Downloads\Fithack\Optimization\processing\typeSegmentation\respiratoryRate_types.py')

# # Define the module name (this can be anything you want to refer to this module as)
# module_name = 'respiratoryRate_types'

# # Load the module using importlib
# spec = importlib.util.spec_from_file_location(module_name, module_path)
# module = importlib.util.module_from_spec(spec)
# sys.modules[module_name] = module
# spec.loader.exec_module(module)

# # Now you can use HeartRateTypeDivisionProcessor from the manually imported module
# RespiratoryRateTypeDivisionProcessor = module.RespiratoryRateTypeDivisionProcessor



class VRespiratoryRateagg:
    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df
        self.workouts_df = workouts_df
        self.user_name = self.records_df['userName'].iloc[0] if 'userName' in self.records_df.columns else 'UnknownUser'
        self.processor_instance = RespiratoryRateTypeDivisionProcessor(self.records_df, self.workouts_df, *args)
        self.processed_df = self.processor_instance.process_data()
        self.s_name = 'V_RespiratoryRate'

    def process_data(self):
        processed_df = self.processed_df

        if processed_df.empty:
            return pd.DataFrame()  # Return empty DataFrame if processed_df is empty

        # Ensure dates and values are properly formatted
        processed_df['startDate'] = pd.to_datetime(processed_df['startDate'], errors='coerce')
        processed_df = processed_df.dropna(subset=['startDate'])
        processed_df['value'] = pd.to_numeric(processed_df['value'], errors='coerce')
        processed_df = processed_df.drop_duplicates(subset='startDate')

        # Initialize flags if missing
        for col in ['activity', 'sleep', 'workout', 'resting']:
            if col not in processed_df.columns:
                processed_df[col] = 0

        # Pre-compute boolean masks for activity, sleep, workout, and resting
        is_activity = processed_df['activity'] == 1
        is_sleep = processed_df['sleep'] == 1
        is_workout = processed_df['workout'] == 1
        is_resting = processed_df['resting'] == 1

        # Perform daily aggregation
        daily_agg = processed_df.groupby(processed_df['startDate'].dt.date).agg(
            dayMin=('value', 'min'),
            dayMax=('value', 'max'),
            dayAvg=('value', 'mean'),
            activityMin=('value', lambda x: x[is_activity].min() if is_activity.any() else pd.NA),
            activityMax=('value', lambda x: x[is_activity].max() if is_activity.any() else pd.NA),
            activityAvg=('value', lambda x: x[is_activity].mean() if is_activity.any() else pd.NA),
            sleepMin=('value', lambda x: x[is_sleep].min() if is_sleep.any() else pd.NA),
            sleepMax=('value', lambda x: x[is_sleep].max() if is_sleep.any() else pd.NA),
            sleepAvg=('value', lambda x: x[is_sleep].mean() if is_sleep.any() else pd.NA),
            workoutMin=('value', lambda x: x[is_workout].min() if is_workout.any() else pd.NA),
            workoutMax=('value', lambda x: x[is_workout].max() if is_workout.any() else pd.NA),
            workoutAvg=('value', lambda x: x[is_workout].mean() if is_workout.any() else pd.NA),
            restingMin=('value', lambda x: x[is_resting].min() if is_resting.any() else pd.NA),
            restingMax=('value', lambda x: x[is_resting].max() if is_resting.any() else pd.NA),
            restingAvg=('value', lambda x: x[is_resting].mean() if is_resting.any() else pd.NA),
        ).reset_index()

        daily_agg.fillna(pd.NA, inplace=True)  # Fill all NA values at once

        # Melt the aggregated DataFrame
        long_format = daily_agg.melt(id_vars=['startDate'], 
                                      value_vars=['dayAvg', 'dayMin', 'dayMax',
                                                  'activityAvg', 'activityMin', 'activityMax', 
                                                  'sleepAvg', 'sleepMin', 'sleepMax',
                                                  'workoutAvg', 'workoutMin', 'workoutMax',
                                                  'restingAvg', 'restingMin', 'restingMax'],
                                      var_name='valueType', 
                                      value_name='value')

        long_format['type'] = processed_df['type'].iloc[0]  # Assuming type is constant
        long_format['unit'] = processed_df['unit'].iloc[0]  # Assuming unit is constant
        long_format['date'] = long_format['startDate']

        final_output = long_format[['date', 'type', 'unit', 'valueType', 'value']]
        final_output['userName'] = self.user_name
        final_output['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_output['value'] = pd.to_numeric(final_output['value']).round(1)  # Convert and round the value column
        final_output['s_name'] = self.s_name
        final_output.sort_values(by=['date', 'type'], ascending=False, ignore_index=True, inplace=True)

        # Reorder columns
        columns_order = ['userName', 'valueGeneratedAt', 's_name'] + [col for col in final_output.columns if col not in ['userName', 'valueGeneratedAt', 's_name']]
        final_output = final_output[columns_order]

        return final_output
    
