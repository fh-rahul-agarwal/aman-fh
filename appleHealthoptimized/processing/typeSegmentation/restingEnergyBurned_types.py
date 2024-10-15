
import pandas as pd
from datetime import datetime, timedelta

class RestingEnergyBurnedTypeDivisionProcessor:
    """
    A processor class for handling and dividing resting calories data into activity, sleep, workout, and resting categories.
    """

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
        return pd.DataFrame(columns=self.workouts_df.columns)

    def _handle_empty_records(self):
        self.flagged_records_df = pd.DataFrame(columns=[
            'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
            'startDate', 'endDate', 'value', 'device', 'activity', 'sleep', 
            'workout', 'resting'
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
        end_of_day = (end_date or start_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()
        return filtered_df.reset_index(drop=True)

    def _flag_records(self):
        """
        Flags records in the filtered DataFrame as activity, sleep, workout, or resting.

        Returns:
        --------
        pd.DataFrame
            The DataFrame with additional columns for activity, sleep, workout, and resting flags.
        """
        if self.filtered_records_df.empty:
            return self.filtered_records_df 
        
        for col in ['activity', 'sleep', 'workout', 'resting']:
            if col not in self.filtered_records_df.columns:
                self.filtered_records_df[col] = 0

        # Flag sleep records first
        self._flag_sleep_records()

        # Flag workout records next
        self._flag_workout_records()

        # Flag activity records last
        self._flag_activity_records()

        # Flag resting records
        self._flag_resting_records()

        return self.filtered_records_df[['userName', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                            'startDate', 'endDate', 'value', 'device', 'activity', 
                                            'sleep', 'workout', 'resting']].reset_index(drop=True)

    def _flag_sleep_records(self):
        """
        Flags sleep records in the filtered DataFrame.
        """
        sleep_values = [            
            'HKCategoryValueSleepAnalysisAsleepCore',
            'HKCategoryValueSleepAnalysisAsleepDeep',
            'HKCategoryValueSleepAnalysisAsleepREM'
        ]
        sleep_mask = self.filtered_records_df['value'].isin(sleep_values)
        sleep_start_times = self.filtered_records_df.loc[sleep_mask, 'startDate']
        sleep_end_times = self.filtered_records_df.loc[sleep_mask, 'endDate']

        for start, end in zip(sleep_start_times, sleep_end_times):
            overlap_mask = (self.filtered_records_df['startDate'] <= end) & (self.filtered_records_df['endDate'] >= start)
            self.filtered_records_df.loc[overlap_mask, 'sleep'] = 1

    def _flag_workout_records(self):
        """
        Flags workout records in the filtered DataFrame.
        """
        for _, workout_row in self.filtered_workouts_df.iterrows():
            workout_start = workout_row['startDate']
            workout_end = workout_row['endDate']
            
            workout_mask = (
                (self.filtered_records_df['startDate'] <= workout_end) &
                (self.filtered_records_df['endDate'] >= workout_start)
            )
            self.filtered_records_df.loc[
                (workout_mask) & (self.filtered_records_df['sleep'] == 0), 
                'workout'
            ] = 1

    def _flag_activity_records(self):
        """
        Flags activity records in the filtered DataFrame.
        """
        activity_types = [
            'HKQuantityTypeIdentifierStepCount',
            'HKQuantityTypeIdentifierDistanceWalkingRunning',
            'HKQuantityTypeIdentifierFlightsClimbed',
            'HKQuantityTypeIdentifierAppleExerciseTime',
            'HKQuantityTypeIdentifierDistanceCycling',
            'HKQuantityTypeIdentifierAppleStandTime',
        ]
        activity_mask = self.filtered_records_df['type'].isin(activity_types)   
        activity_start_times = self.filtered_records_df.loc[activity_mask, 'startDate']
        activity_end_times = self.filtered_records_df.loc[activity_mask, 'endDate']

        for start, end in zip(activity_start_times, activity_end_times):
            overlap_mask = (self.filtered_records_df['startDate'] <= end) & (self.filtered_records_df['endDate'] >= start)
            self.filtered_records_df.loc[
                overlap_mask & (self.filtered_records_df['sleep'] == 0) & (self.filtered_records_df['workout'] == 0), 
                'activity'
            ] = 1

    def _flag_resting_records(self):
        """
        Flags resting records in the filtered DataFrame.
        """
        # Resting is defined where none of the other flags are set
        self.filtered_records_df['resting'] = (
            (self.filtered_records_df['activity'] == 0) & 
            (self.filtered_records_df['sleep'] == 0) & 
            (self.filtered_records_df['workout'] == 0)
        ).astype(int)


    def process_data(self):
        resting_calories_df = self.filtered_records_df[self.filtered_records_df['type'] == 'HKQuantityTypeIdentifierBasalEnergyBurned'].copy()
        if resting_calories_df.empty:
            return resting_calories_df  # Return empty DataFrame if no resting calories data

        # Set generation timestamp and sort data
        resting_calories_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        resting_calories_df['value'] = pd.to_numeric(resting_calories_df['value'], errors='coerce').round(3)
        
        resting_calories_df['startDate'] = pd.to_datetime(resting_calories_df['startDate'])
        resting_calories_df['dateSorting'] = resting_calories_df['startDate'].dt.date
        
        resting_calories_df = resting_calories_df.sort_values(by=['dateSorting', 'type'], ascending=False, ignore_index=True)

        return resting_calories_df[['type', 'userName', 'value', 'valueGeneratedAt', 'sourceName', 'sourceVersion', 'unit', 'creationDate','startDate', 'endDate', 'activity', 'sleep', 'workout', 'resting', ]]
    

# records_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv')
# workouts_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv')

# args = ['2024-09-13', 10, '-']

# processor = RestingEnergyBurnedTypeDivisionProcessor(records_df, workouts_df, *args)
# result_df = processor.process_data()
# print(result_df)
