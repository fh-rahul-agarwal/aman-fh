import pandas as pd
import numpy as np
from datetime import datetime, timedelta
class ActiveEnergyBurnedTypeDivisionProcessor:

    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df.copy()
        self.workouts_df = workouts_df.copy()
        
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_records_df = self._filter_by_dates_list(self.records_df, self.dates_list)
            self.filtered_workouts_df = self._filter_by_dates_list(self.workouts_df, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_records_df = self._filter_by_single_date(self.records_df, self.start_date)
            self.filtered_workouts_df = self._filter_by_single_date(self.workouts_df, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_records_df = self._filter_by_date_range(self.records_df, self.start_date, self.end_date)
            self.filtered_workouts_df = self._filter_by_date_range(self.workouts_df, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.filtered_records_df = self._filter_by_offset(self.records_df, self.start_date, self.days_offset, self.offset_sign)
            self.filtered_workouts_df = self._filter_by_offset(self.workouts_df, self.start_date, self.days_offset, self.offset_sign)
        
        if not self.filtered_records_df.empty:
            if self.filtered_workouts_df.empty:
                self.filtered_workouts_df = self._initialize_empty_workout_df()
            self.flagged_records_df = self._flag_records()
        else:
            self._handle_empty_records()

    def _initialize_empty_workout_df(self):
        """
        Returns an empty DataFrame with the structure of the workouts DataFrame.
        """
        return pd.DataFrame(columns=self.workouts_df.columns)

    def _handle_empty_records(self):
        """
        Handles the case when the filtered records DataFrame is empty.
        """
        self.flagged_records_df = pd.DataFrame(columns=[
            'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 'startDate', 
            'endDate', 'value', 'device', 'activity', 'sleep', 'workout', 'resting'
        ])
        
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

    def _flag_records(self):
        """Flags records as activity or workout."""
        if self.filtered_records_df.empty:
            return self.filtered_records_df
 
        self.filtered_records_df['activity'] = 0
        self.filtered_records_df['workout'] = 0

        for _, workout_row in self.filtered_workouts_df.iterrows():
            workout_start = workout_row['startDate']
            workout_end = workout_row['endDate']
            
            workout_overlap_mask = (
                (self.filtered_records_df['startDate'] <= workout_end) & 
                (self.filtered_records_df['endDate'] >= workout_start)
            )
            
            self.filtered_records_df.loc[workout_overlap_mask, 'workout'] = 1

        self.filtered_records_df.loc[self.filtered_records_df['workout'] == 0, 'activity'] = 1

        return self.filtered_records_df[['userName', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                        'startDate', 'endDate', 'value', 'device', 'activity', 
                                        'workout']].reset_index(drop=True)


    def process_data(self):
        """Processes active energy burned data."""
        active_energy_df = self.filtered_records_df[self.filtered_records_df['type'] == 'HKQuantityTypeIdentifierActiveEnergyBurned'].copy()
        if active_energy_df.empty:
            return active_energy_df
        
        # Flag the active energy records
        active_energy_df = self._flag_active_energy_records(active_energy_df)

        # Generate value timestamp and sort by date
        active_energy_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        active_energy_df['startDate'] = pd.to_datetime(active_energy_df['startDate'])
        active_energy_df['dateSorting'] = active_energy_df['startDate'].dt.date
        active_energy_df.sort_values(by=['dateSorting', 'type'], ascending=False, inplace=True)

        # Convert and round values, initialize resting and sleep to 0
        active_energy_df['value'] = pd.to_numeric(active_energy_df['value']).round(3)
        active_energy_df[['resting', 'sleep']] = 0  

        return active_energy_df[['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                 'startDate', 'endDate', 'value', 'activity', 'sleep', 'workout', 'resting']].reset_index(drop=True)

    def _flag_active_energy_records(self, active_energy_df):
        """Returns the active energy records with flags added."""
        return active_energy_df[['userName', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                 'startDate', 'endDate', 'value', 'activity', 'workout']].copy()



# records_df = pd.read_csv(r"C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv")
# workouts_df = pd.read_csv(r"C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv")

# args = ['2024-09-13', 10, '-']
# processor = ActiveEnergyBurnedTypeDivisionProcessor(records_df, workouts_df, *args)
# result_df = processor.process_data()
# print(result_df)