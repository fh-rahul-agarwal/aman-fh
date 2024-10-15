
import pandas as pd
from datetime import datetime, timedelta

class HRVTypeDivisionProcessor:
    """
    A processor class for handling and dividing heart rate variability (HRV) data into 
    various categories such as activity, sleep, workout, and resting based on the provided records and workouts.

    Parameters:
    -----------
    records_df : pd.DataFrame
        DataFrame containing the health records, including HRV data.
    workouts_df : pd.DataFrame
        DataFrame containing workout data.
    start_date : str or pd.Timestamp
        The date from which to start filtering and processing the data.
    
    Attributes:
    -----------
    filtered_records_df : pd.DataFrame
        DataFrame containing filtered records based on the start date.
    filtered_workouts_df : pd.DataFrame
        DataFrame containing filtered workout records based on the start date.
    flagged_records_df : pd.DataFrame
        DataFrame containing records with flags for activity, sleep, workout, and resting.
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

    def _filter_by_single_date(self, df, start_date):
        return self._filter_data(df, start_date)

    def _filter_by_date_range(self, df, start_date, end_date):
        return self._filter_data(df, start_date, end_date)

    def _filter_by_offset(self, df, start_date, days_offset, offset_sign):
        if offset_sign == '+':
            end_date = start_date + timedelta(days=days_offset)
        else:  # offset_sign == '-'
            end_date = start_date
            start_date = start_date - timedelta(days=days_offset)
        return self._filter_data(df, start_date, end_date)

    def _filter_by_dates_list(self, df, dates_list):
        return pd.concat([self._filter_data(df, date) for date in dates_list]).drop_duplicates().reset_index(drop=True)

    def _filter_data(self, df, start_date, end_date=None):
        if df.empty:
            return pd.DataFrame(columns=['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device'])

        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = (end_date or start_date).replace(hour=23, minute=59, second=59, microsecond=999999)

        filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)].copy()
        return filtered_df.reset_index(drop=True)
    
    def _initialize_empty_workout_df(self):
        return pd.DataFrame(columns=['startDate', 'endDate'], dtype='datetime64[ns]')

    def _handle_empty_records(self):
        message = f'No records found for the date: {self.start_date}' if not self.workouts_df.empty else f'No data available for the date: {self.start_date}'
        print(message)

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
        
        self.filtered_records_df = self._initialize_flags(self.filtered_records_df)
        self._flag_sleep_records()
        self._flag_workout_records()
        self._flag_activity_records()
        self._flag_resting_records()

        return self.filtered_records_df[['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device', 'activity', 
                                         'sleep', 'workout', 'resting']].reset_index(drop=True)

    def _initialize_flags(self, df):
        """
        Initializes the activity, sleep, workout, and resting flags in the DataFrame.

        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame to initialize the flags.

        Returns:
        --------
        pd.DataFrame
            The DataFrame with initialized flag columns.
        """
        flags = ['activity', 'sleep', 'workout', 'resting']
        for col in flags:
            df[col] = 0
        return df
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
        workout_start_times = self.filtered_workouts_df['startDate']
        workout_end_times = self.filtered_workouts_df['endDate']

        for start, end in zip(workout_start_times, workout_end_times):
            workout_mask = (self.filtered_records_df['startDate'] <= end) & (self.filtered_records_df['endDate'] >= start)
            
            self.filtered_records_df.loc[workout_mask & (self.filtered_records_df['sleep'] == 0), 'workout'] = 1
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
        self.filtered_records_df['resting'] = (
            (self.filtered_records_df['activity'] == 0) & 
            (self.filtered_records_df['sleep'] == 0) & 
            (self.filtered_records_df['workout'] == 0)
        ).astype(int)

    def process_data(self):
        """
        Processes the HRV data by filtering and flagging the relevant records.

        Returns:
        --------
        pd.DataFrame
            The DataFrame containing HRV records with flags for activity, sleep, workout, and resting.
        """
        hrv_df = self.filtered_records_df[self.filtered_records_df['type'] == 'HKQuantityTypeIdentifierHeartRateVariabilitySDNN'].copy()
        if hrv_df.empty:
            return hrv_df
        
        hrv_df = self._flag_hrv_records(hrv_df)
        hrv_df['value'] = pd.to_numeric(hrv_df['value'], errors='coerce').round(1)  # Convert value column to float and round
        return hrv_df.reset_index(drop=True)

    def _flag_hrv_records(self, hrv_df):
        """
        Reuses flagged data to flag the HRV records specifically.

        Parameters:
        -----------
        hrv_df : pd.DataFrame
            The DataFrame containing HRV records.

        Returns:
        --------
        pd.DataFrame
            The flagged HRV DataFrame.
        """
        hrv_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        hrv_df['startDate'] = pd.to_datetime(hrv_df['startDate']).dt.tz_localize(None)
        hrv_df['endDate'] = pd.to_datetime(hrv_df['endDate']).dt.tz_localize(None)

        # Adding flags based on existing flags in the filtered records
        hrv_df['activity'] = self.filtered_records_df['activity']
        hrv_df['sleep'] = self.filtered_records_df['sleep']
        hrv_df['workout'] = self.filtered_records_df['workout']
        hrv_df['resting'] = self.filtered_records_df['resting']
        
        return hrv_df[['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                       'startDate', 'endDate', 'value', 'activity', 
                       'sleep', 'workout', 'resting']].reset_index(drop=True)
    

# records_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv')
# workouts_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv')
# args = ['2024-09-15', 2, '-']

# processor = HRVTypeDivisionProcessor(records_df, workouts_df, *args)
# result_df = processor.process_data()
# print(result_df.columns)