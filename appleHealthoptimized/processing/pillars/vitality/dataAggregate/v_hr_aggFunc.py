
import pandas as pd
from datetime import datetime
from processing.typeSegmentation.heartRate_types import HeartRateTypeDivisionProcessor



class VHRagg:
    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df
        self.workouts_df = workouts_df
        self.user_name = self.records_df['userName'].iloc[0] if 'userName' in self.records_df.columns else 'UnknownUser'
        self.classProcessorName = HeartRateTypeDivisionProcessor
        # Create an instance of HeartRateTypeDivisionProcessor
        self.processor_instance = self.classProcessorName(self.records_df, self.workouts_df, *args)
        # Process data using the instance
        self.processed_df = self.processor_instance.process_data()
        self.s_name = 'V_HR'

    def process_data(self):
        processed_df = self.processed_df

        # Ensure processed_df is valid
        if processed_df.empty:
            return pd.DataFrame()  # Return an empty DataFrame if there's no data

        # Handle date and value conversions
        processed_df['startDate'] = pd.to_datetime(processed_df['startDate'], errors='coerce')
        processed_df.dropna(subset=['startDate'], inplace=True)
        processed_df['value'] = pd.to_numeric(processed_df['value'], errors='coerce')
        processed_df.drop_duplicates(subset='startDate', inplace=True)

        # Initialize missing columns
        for col in ['activity', 'sleep', 'workout', 'resting']:
            if col not in processed_df.columns:
                processed_df[col] = 0

        # Use groupby and agg to compute daily aggregations
        daily_agg = processed_df.groupby(processed_df['startDate'].dt.date).agg(
            dayMin=('value', 'min'),
            dayMax=('value', 'max'),
            dayAvg=('value', 'mean'),
            activityMin=('value', lambda x: x[processed_df.loc[x.index, 'activity'] == 1].min()),
            activityMax=('value', lambda x: x[processed_df.loc[x.index, 'activity'] == 1].max()),
            activityAvg=('value', lambda x: x[processed_df.loc[x.index, 'activity'] == 1].mean()),
            sleepMin=('value', lambda x: x[processed_df.loc[x.index, 'sleep'] == 1].min()),
            sleepMax=('value', lambda x: x[processed_df.loc[x.index, 'sleep'] == 1].max()),
            sleepAvg=('value', lambda x: x[processed_df.loc[x.index, 'sleep'] == 1].mean()),
            workoutMin=('value', lambda x: x[processed_df.loc[x.index, 'workout'] == 1].min()),
            workoutMax=('value', lambda x: x[processed_df.loc[x.index, 'workout'] == 1].max()),
            workoutAvg=('value', lambda x: x[processed_df.loc[x.index, 'workout'] == 1].mean()),
            restingMin=('value', lambda x: x[processed_df.loc[x.index, 'resting'] == 1].min()),
            restingMax=('value', lambda x: x[processed_df.loc[x.index, 'resting'] == 1].max()),
            restingAvg=('value', lambda x: x[processed_df.loc[x.index, 'resting'] == 1].mean()),
        ).reset_index()

        # Handle missing values
        daily_agg.fillna(pd.NA, inplace=True)

        # Melt the DataFrame to long format
        long_format = daily_agg.melt(
            id_vars=['startDate'], 
            value_vars=[
                'dayAvg', 'dayMin', 'dayMax',
                'activityAvg', 'activityMin', 'activityMax', 
                'sleepAvg', 'sleepMin', 'sleepMax',
                'workoutAvg', 'workoutMin', 'workoutMax',
                'restingAvg', 'restingMin', 'restingMax'
            ],
            var_name='valueType', 
            value_name='value'
        )

        # Add additional information to long format
        long_format['type'] = processed_df['type'].iloc[0] if 'type' in processed_df.columns else None
        long_format['unit'] = processed_df['unit'].iloc[0] if 'unit' in processed_df.columns else None
        long_format['date'] = long_format['startDate']

        # Define output DataFrame order
        final_output = long_format[['date', 'type', 'unit', 'valueType', 'value']]
        final_output['s_name'] = self.s_name
        final_output['userName'] = self.user_name
        final_output['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        final_output['value'] = pd.to_numeric(long_format['value']).round(1)
        final_output.sort_values(by=['date', 'type'], ascending=False, inplace=True, ignore_index=True)

        # Reorder columns to desired format
        columns_order = ['userName', 'valueGeneratedAt', 's_name'] + [col for col in final_output.columns if col not in ['userName', 'valueGeneratedAt', 's_name']]
        final_output = final_output[columns_order]

        return final_output
    

# import sys
# import importlib.util
# import os

# # Specify the path to the directory containing the module
# # For example, if 'typeSegmentation' is in 'C:\Users\amank\Downloads\Fithack\Optimization\processing'
# module_path = os.path.abspath(r'C:\Users\amank\Downloads\Fithack\Optimization\processing\typeSegmentation\heartRate_types.py')

# # Define the module name (this can be anything you want to refer to this module as)
# module_name = 'heartRate_types'

# spec = importlib.util.spec_from_file_location(module_name, module_path)
# module = importlib.util.module_from_spec(spec)
# sys.modules[module_name] = module
# spec.loader.exec_module(module)

# # Now you can use HeartRateTypeDivisionProcessor from the manually imported module
# HeartRateTypeDivisionProcessor = module.HeartRateTypeDivisionProcessor

# records_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv', low_memory=False)
# workouts_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv', low_memory=False)

# args = ['2024-09-13', 10, '-']

# processor = VHRagg(records_df, workouts_df, *args) 
# result_df = processor.process_data()
# print(result_df)