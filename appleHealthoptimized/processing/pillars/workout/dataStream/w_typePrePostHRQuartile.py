import pandas as pd
from datetime import datetime, timedelta

class WPrePostHRQuartile:

    def __init__(self, records_df, workouts_df, *args, pre_post_quartiles=2):
        self.workouts_df = workouts_df.copy()
        self.records_df = records_df.copy()
        self.pre_post_quartiles = pre_post_quartiles
        self.user_name = self.workouts_df['userName'].iloc[0] if 'userName' in self.workouts_df.columns else 'UnknownUser'
        self.filtered_records_df = pd.DataFrame(columns=records_df.columns)  # Initialize empty DataFrame

        # Filter records based on provided arguments
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

        if self.filtered_records_df.empty:
            self._handle_empty_records()    

        # Convert and prepare filtered_workouts_df
        self.filtered_workouts_df["startDate"] = pd.to_datetime(self.filtered_workouts_df["startDate"])
        self.filtered_workouts_df["endDate"] = pd.to_datetime(self.filtered_workouts_df["endDate"])
        self.filtered_workouts_df["Date"] = self.filtered_workouts_df["startDate"].dt.date
        self.filtered_workouts_df["Duration"] = self.filtered_workouts_df["endDate"] - self.filtered_workouts_df["startDate"]
        self.filtered_workouts_df["Duration (Hours)"] = self.filtered_workouts_df["Duration"].apply(lambda x: round(x.total_seconds() / 3600, 1))
        
        # Process heart rate data
        self.heart_rate_df = self.filtered_records_df[self.filtered_records_df["type"] == 'HKQuantityTypeIdentifierHeartRate'].copy()
        self.heart_rate_df['startDate'] = pd.to_datetime(self.heart_rate_df['startDate'])
        self.heart_rate_df['endDate'] = pd.to_datetime(self.heart_rate_df['endDate'])
        self.heart_rate_df["value"] = pd.to_numeric(self.heart_rate_df['value'])

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
            return pd.DataFrame(columns=['type', 'sourceName', 'sourceVersion', 'unit', 'creationDate', 
                                         'startDate', 'endDate', 'value', 'device'])

        df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
        df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['endDate'] <= end_of_day)].copy()
        else:
            end_of_day = start_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['endDate'] <= end_of_day)].copy()

        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        if not self.filtered_workouts_df.empty:
            print(f'No workout records found for the date: {self.start_date}')
        else:
            print(f'No data available for the date: {self.start_date}')    

    def process(self):
        # Initialize list to store results
        activity_heart_rate = []

        # Process each workout
        for _, workout in self.filtered_workouts_df.iterrows():
            workout_date = workout['creationDate']
            workout_type = workout['workoutActivityType']
            start_time = workout['startDate']
            end_time = workout['endDate']
            duration = workout['Duration (Hours)'] * 3600  # Convert duration to seconds

            # Calculate the workout quartile duration
            workout_quartile_duration = duration / 4

            # Pre-workout processing
            self._process_quartiles(
                activity_heart_rate, 
                phase='PreWorkout', 
                quartile_duration=workout_quartile_duration, 
                num_quartiles=self.pre_post_quartiles, 
                reference_time=start_time - pd.to_timedelta(workout_quartile_duration * self.pre_post_quartiles, unit='s'),
                filtered_heart_rate=self.heart_rate_df[self.heart_rate_df['startDate'] < start_time],
                workout_date=workout_date,
                workout_type=workout_type
            )

            # Workout processing
            self._process_quartiles(
                activity_heart_rate, 
                phase='Workout', 
                quartile_duration=workout_quartile_duration, 
                num_quartiles=4,  # Workout will always have 4 quartiles
                reference_time=start_time,
                filtered_heart_rate=self.heart_rate_df[(self.heart_rate_df['startDate'] >= start_time) & (self.heart_rate_df['startDate'] <= end_time)],
                workout_date=workout_date,
                workout_type=workout_type
            )

            # Post-workout processing
            self._process_quartiles(
                activity_heart_rate, 
                phase='PostWorkout', 
                quartile_duration=workout_quartile_duration, 
                num_quartiles=self.pre_post_quartiles, 
                reference_time=end_time,
                filtered_heart_rate=self.heart_rate_df[self.heart_rate_df['startDate'] > end_time],
                workout_date=workout_date,
                workout_type=workout_type
            )

        # Convert the results to a DataFrame
        activity_heart_rate_df = pd.DataFrame(activity_heart_rate)

        # Drop duplicates based on Date, Workout Type, Phase, and Quartile
        activity_heart_rate_df_unique = activity_heart_rate_df.drop_duplicates(subset=['creationDate', 'workoutActivityType', 'phase', 'quartile'])
        activity_heart_rate_df_unique = activity_heart_rate_df_unique.sort_values(by=['creationDate', 'sourceName'], ascending=False, ignore_index=True) 

        return activity_heart_rate_df_unique

    def _process_quartiles(self, activity_heart_rate, phase, quartile_duration, num_quartiles, reference_time, filtered_heart_rate, workout_date, workout_type):
        for i in range(num_quartiles):
            quartile_start_time = reference_time + pd.to_timedelta(i * quartile_duration, unit='s')
            quartile_end_time = reference_time + pd.to_timedelta((i + 1) * quartile_duration, unit='s')

            quartile_heart_rate = filtered_heart_rate[(filtered_heart_rate['startDate'] >= quartile_start_time) & (filtered_heart_rate['startDate'] < quartile_end_time)]

            # Default values for quartile statistics
            min_heart_rate = max_heart_rate = avg_heart_rate = float('nan')

            if not quartile_heart_rate.empty:
                min_heart_rate = round(quartile_heart_rate['value'].min(), 1)
                max_heart_rate = round(quartile_heart_rate['value'].max(), 1)
                avg_heart_rate = round(quartile_heart_rate['value'].mean(), 1)

            # Collect unique source names as a list
            source_names = filtered_heart_rate['sourceName'].unique().tolist()
            source_names_str = ', '.join(source_names)

            activity_heart_rate.append({
                'userName': self.user_name,
                'valueGeneratedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'sourceName': source_names_str,
                'creationDate': workout_date,
                'workoutActivityType': workout_type,
                'phase': phase,
                'quartile': f'Q{i+1}',
                'startTime': quartile_start_time,
                'endTime': quartile_end_time,
                'minHeartRate': min_heart_rate,
                'maxHeartRate': max_heart_rate,
                'avgHeartRate': avg_heart_rate,
            })
