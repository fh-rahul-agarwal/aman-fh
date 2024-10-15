import pandas as pd
from datetime import datetime, timedelta

class HeartRateTypeDivisionProcessor:
    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df
        self.workouts_df = workouts_df
        
        # Determine filtering method based on arguments
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

        # Initialize empty workout DataFrame if necessary
        if self.filtered_records_df.empty:
            self._handle_empty_records()
        else:
            self.filtered_workouts_df = self._initialize_empty_workout_df() if self.filtered_workouts_df.empty else self.filtered_workouts_df
            self.flagged_records_df = self._flag_records()

    def _filter_by_single_date(self, df, start_date):
        return self._filter_data(df, start_date)

    def _filter_by_date_range(self, df, start_date, end_date):
        return self._filter_data(df, start_date, end_date)

    def _filter_by_offset(self, df, start_date, days_offset, offset_sign):
        end_date = start_date + timedelta(days=days_offset) if offset_sign == '+' else start_date
        start_date = start_date - timedelta(days=days_offset) if offset_sign == '-' else start_date
        return self._filter_data(df, start_date, end_date)

    def _filter_by_dates_list(self, df, dates_list):
        return pd.concat([self._filter_data(df, date) for date in dates_list]).drop_duplicates().reset_index(drop=True)

    def _filter_data(self, df, start_date, end_date=None):
        if df.empty:
            return pd.DataFrame(columns=['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device'])
        
        # Convert startDate and endDate to datetime
        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        # Create date boundaries
        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = (end_date or start_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        # Apply filtering
        return df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].reset_index(drop=True)

    def _initialize_empty_workout_df(self):
        return pd.DataFrame(columns=['startDate', 'endDate'])

    def _handle_empty_records(self):
        if not self.workouts_df.empty:
            print(f'No records found for the date: {self.start_date}')
        else:
            print(f'No data available for the date: {self.start_date}')

    def _flag_records(self):
        if self.filtered_records_df.empty:
            return self.filtered_records_df
        
        # Initialize flags
        self.filtered_records_df[['activity', 'sleep', 'workout', 'resting']] = 0

        # Flag records in one go using vectorized operations
        self._flag_sleep_records()
        self._flag_workout_records()
        self._flag_activity_records()
        self._flag_resting_records()

        return self.filtered_records_df[['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device', 'activity', 
                                         'sleep', 'workout', 'resting']].reset_index(drop=True)

    def _flag_sleep_records(self):
        sleep_values = [
            'HKCategoryValueSleepAnalysisAsleepCore',
            'HKCategoryValueSleepAnalysisAsleepDeep',
            'HKCategoryValueSleepAnalysisAsleepREM'
        ]
        sleep_mask = self.filtered_records_df['value'].isin(sleep_values)
        sleep_start_times = self.filtered_records_df.loc[sleep_mask, 'startDate']
        sleep_end_times = self.filtered_records_df.loc[sleep_mask, 'endDate']

        # Create a mask for overlaps
        for start, end in zip(sleep_start_times, sleep_end_times):
            overlap_mask = (self.filtered_records_df['startDate'] <= end) & (self.filtered_records_df['endDate'] >= start)
            self.filtered_records_df.loc[overlap_mask, 'sleep'] = 1

    def _flag_workout_records(self):
        for _, workout_row in self.filtered_workouts_df.iterrows():
            workout_mask = (
                (self.filtered_records_df['startDate'] <= workout_row['endDate']) & 
                (self.filtered_records_df['endDate'] >= workout_row['startDate'])
            )
            self.filtered_records_df.loc[workout_mask & (self.filtered_records_df['sleep'] == 0), 'workout'] = 1

    def _flag_activity_records(self):
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
            overlap_mask = (
                (self.filtered_records_df['startDate'] <= end) & 
                (self.filtered_records_df['endDate'] >= start)
            )
            self.filtered_records_df.loc[overlap_mask & (self.filtered_records_df['sleep'] == 0) & (self.filtered_records_df['workout'] == 0), 'activity'] = 1

    def _flag_resting_records(self):
        self.filtered_records_df['resting'] = (
            (self.filtered_records_df['activity'] == 0) & 
            (self.filtered_records_df['sleep'] == 0) & 
            (self.filtered_records_df['workout'] == 0)
        ).astype(int)

    def process_data(self):
        heart_rate_df = self.filtered_records_df[self.filtered_records_df['type'] == 'HKQuantityTypeIdentifierHeartRate'].copy()
        if heart_rate_df.empty:
            return heart_rate_df
        
        heart_rate_df = self._flag_heart_rate_records(heart_rate_df)
        heart_rate_df['value'] = heart_rate_df['value'].astype(float).round(1)
        return heart_rate_df.reset_index(drop=True)

    def _flag_heart_rate_records(self, heart_rate_df):
        heart_rate_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        heart_rate_df['startDate'] = pd.to_datetime(heart_rate_df['startDate'])
        heart_rate_df['dateSorting'] = heart_rate_df['startDate'].dt.date
        heart_rate_df = heart_rate_df.sort_values(by=['dateSorting', 'type'], ascending=False).reset_index(drop=True)
        return heart_rate_df[['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'unit', 
                              'creationDate', 'startDate', 'endDate', 'value', 'activity', 
                              'sleep', 'workout', 'resting']].copy()



