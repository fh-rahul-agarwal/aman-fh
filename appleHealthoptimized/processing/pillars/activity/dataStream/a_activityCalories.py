import pandas as pd
from datetime import timedelta
from processing.typeSegmentation.activeEnergyBurned_types import *

class AActivityCalories:

    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df.copy()
        self.workouts_df = workouts_df.copy()
        self.args = args
        self.filtered_activity_records_df = self._filter_activity_records()
        
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_records_df = self._filter_by_dates_list(self.records_df, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_records_df = self._filter_by_single_date(self.records_df, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_records_df = self._filter_by_date_range(self.records_df, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.filtered_records_df = self._filter_by_offset(self.records_df, self.start_date, self.days_offset, self.offset_sign)
        
        if self.filtered_records_df.empty:
            self._handle_empty_records()

    def _filter_activity_records(self):
        """
        Filters the DataFrame to include only activity records.
        
        Returns:
        --------
        pd.DataFrame
            The filtered DataFrame with activity records.
        """
        activity_df = ActiveEnergyBurnedTypeDivisionProcessor(self.records_df, self.workouts_df, *self.args).process_data()
        # Ensure startDate and endDate are timezone-naive
        activity_df['startDate'] = activity_df['startDate'].dt.tz_localize(None)
        activity_df['endDate'] = activity_df['endDate'].dt.tz_localize(None)
        return activity_df[activity_df['activity'] == 1].copy()        

    def _handle_empty_records(self):
        """
        Handles the case when the filtered records DataFrame is empty.
        """
        print("No Records data available for the specified dates.")
        
    def _filter_by_single_date(self, df, start_date):
        return self._filter_data(df, start_date)

    def _filter_by_date_range(self, df, start_date, end_date):
        return self._filter_data(df, start_date, end_date)

    def _filter_by_offset(self, df, start_date, days_offset, offset_sign):
        if offset_sign == '+':
            end_date = start_date + timedelta(days=days_offset)
        elif offset_sign == '-':
            end_date = start_date
            start_date = start_date - timedelta(days=days_offset)
        return self._filter_data(df, start_date, end_date)

    def _filter_by_dates_list(self, df, dates_list):
        filtered_df = pd.concat([self._filter_data(df, date) for date in dates_list])
        return filtered_df.drop_duplicates().reset_index(drop=True)

    def _filter_data(self, df, start_date, end_date=None):
        if df.empty:
            return pd.DataFrame(columns=['userName', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device'])

        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()
        else:
            end_of_day = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()

        return filtered_df.reset_index(drop=True)    

    def process(self):
        """
        Processes the data by flagging specific activity types and applying priority.
        
        Returns:
        --------
        pd.DataFrame
            The DataFrame with flagged activity columns based on priority.
        """
        if self.filtered_activity_records_df.empty:
            return self.filtered_activity_records_df

        self.filtered_activity_records_df = self._initialize_flags(self.filtered_activity_records_df)
        self._flag_activity_types()
        self._apply_priority()  # Apply priority logic to resolve overlaps

        final_activity_records_df = self.filtered_activity_records_df.copy()

        final_activity_records_df['creationDate'] = pd.to_datetime(final_activity_records_df['creationDate'])
        final_activity_records_df['dateSorting'] = final_activity_records_df['creationDate'].dt.date
        final_activity_records_df = final_activity_records_df.sort_values(by=['dateSorting', 'type'], ascending=False, ignore_index=True)
        
        return final_activity_records_df[['userName', 'valueGeneratedAt', 'type', 'sourceName', 
                                         'sourceVersion', 'unit', 'creationDate', 'startDate', 
                                         'endDate', 'value', 'flightsClimbed', 'stepCount', 
                                         'standTime', 'otherActivities']].reset_index(drop=True)

    def _initialize_flags(self, df):
        """
        Initializes the activity flags in the DataFrame.
        
        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame to initialize the flags.

        Returns:
        --------
        pd.DataFrame
            The DataFrame with initialized flag columns.
        """
        for col in ['flightsClimbed', 'stepCount', 'standTime', 'otherActivities']:
            if col not in df.columns:
                df[col] = 0
        return df

    def _flag_activity_types(self):
        """
        Flags the records based on activity types.
        """
        activity_types = {
            'flightsClimbed': 'HKQuantityTypeIdentifierFlightsClimbed',
            'stepCount': 'HKQuantityTypeIdentifierStepCount',
            'standTime': 'HKQuantityTypeIdentifierAppleStandTime'
        }
        
        for flag, activity_type in activity_types.items():
            self._flag_specific_activity(activity_type, flag)
        
        self._flag_other_activities()

    def _flag_specific_activity(self, activity_type, flag):
        """
        Flags the records for a specific activity type.
        
        Parameters:
        -----------
        activity_type : str
            The activity type identifier.
        flag : str
            The corresponding flag column to update.
        """
        activity_df = self.filtered_records_df[self.filtered_records_df['type'] == activity_type].copy()
        
        # Ensure startDate and endDate are datetime objects
        if not pd.api.types.is_datetime64_any_dtype(activity_df['startDate']):
            activity_df['startDate'] = pd.to_datetime(activity_df['startDate'])
        if not pd.api.types.is_datetime64_any_dtype(activity_df['endDate']):
            activity_df['endDate'] = pd.to_datetime(activity_df['endDate'])

        # Ensure startDate and endDate are timezone-naive
        activity_df['startDate'] = activity_df['startDate'].dt.tz_localize(None)
        activity_df['endDate'] = activity_df['endDate'].dt.tz_localize(None)

        for _, activity_row in activity_df.iterrows():
            mask = (
                (self.filtered_activity_records_df['startDate'] <= activity_row['endDate']) &
                (self.filtered_activity_records_df['endDate'] >= activity_row['startDate'])
            )
            self.filtered_activity_records_df.loc[mask, flag] = 1

    def _flag_other_activities(self):
        """
        Flags the records that do not match the specific activities as 'otherActivities'.
        """
        specific_flags = ['flightsClimbed', 'stepCount', 'standTime']
        other_mask = self.filtered_activity_records_df[specific_flags].sum(axis=1) == 0
        self.filtered_activity_records_df.loc[other_mask, 'otherActivities'] = 1

    def _apply_priority(self):
        """
        Ensures only one flag is set per row based on the priority:
        flightsClimbed > stepCount > standTime > otherActivities.
        """
        for i, row in self.filtered_activity_records_df.iterrows():
            if row['flightsClimbed'] == 1:
                self.filtered_activity_records_df.loc[i, ['stepCount', 'standTime', 'otherActivities']] = 0
            elif row['stepCount'] == 1:
                self.filtered_activity_records_df.loc[i, ['standTime', 'otherActivities']] = 0
            elif row['standTime'] == 1:
                self.filtered_activity_records_df.loc[i, ['otherActivities']] = 0

# import sys
# import importlib.util
# import os

# # Specify the path to the directory containing the module
# # For example, if 'typeSegmentation' is in 'C:\Users\amank\Downloads\Fithack\Optimization\processing'
# module_path = os.path.abspath(r'C:\Users\amank\Downloads\Fithack\Optimization\processing\typeSegmentation\activeEnergyBurned_types.py')

# # Define the module name (this can be anything you want to refer to this module as)
# module_name = 'activeEnergyBurned_types'

# spec = importlib.util.spec_from_file_location(module_name, module_path)
# module = importlib.util.module_from_spec(spec)
# sys.modules[module_name] = module
# spec.loader.exec_module(module)

# # Now you can use HeartRateTypeDivisionProcessor from the manually imported module
# ActiveEnergyBurnedTypeDivisionProcessor = module.ActiveEnergyBurnedTypeDivisionProcessor

# records_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv')
# workouts_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv')

# args = ['2024-09-13', 10, '-']

# processor = AActivityCalories(records_df, workouts_df, *args) 
# result_df = processor.process()
# print(result_df)