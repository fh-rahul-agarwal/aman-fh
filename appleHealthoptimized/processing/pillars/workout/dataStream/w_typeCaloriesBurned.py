import pandas as pd
from datetime import datetime, timedelta

class WCalories:

    def __init__(self, records_df, workouts_df, *args):
            
        self.records_df = records_df.copy()
        self.workouts_df = workouts_df.copy()
        self.user_name = self.workouts_df['userName'].iloc[0] if 'userName' in self.workouts_df.columns else 'UnknownUser'

        self.calories_mapping = {
            'HKQuantityTypeIdentifierActiveEnergyBurned': 'Active Energy Burned',
        }
        
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

        self._process_workout_data()
        self._process_calories_data()

        if self.filtered_records_df.empty:
            self._handle_empty_records()

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

    def _process_workout_data(self):
        """
        Processes workout data by:
        - Replacing workout activity types with user-friendly names.
        - Converting date columns to datetime format without timezone information.
        - Creating 'creationDate' and 'Duration' columns.
        - Calculating 'Duration (Minutes)' from the start and end dates of workouts.
        """
        self.filtered_workouts_df['workoutActivityType'] = self.filtered_workouts_df['workoutActivityType']

        # Convert date columns to datetime without timezone
        self.filtered_workouts_df["startDate"] = pd.to_datetime(self.filtered_workouts_df["startDate"]).dt.tz_localize(None)
        self.filtered_workouts_df["endDate"] = pd.to_datetime(self.filtered_workouts_df["endDate"]).dt.tz_localize(None)
        
        # Create Date and Duration columns
        self.filtered_workouts_df["Date"] = self.filtered_workouts_df["startDate"].dt.date
        self.filtered_workouts_df["Duration"] = self.filtered_workouts_df["endDate"] - self.filtered_workouts_df["startDate"]
        self.filtered_workouts_df["Duration (Minutes)"] = self.filtered_workouts_df["Duration"].apply(lambda x: round(x.total_seconds() / 60, 1))

    def _process_calories_data(self):
        """
        Processes calorie records data by:
        - Filtering and mapping calorie types.
        - Converting the 'startDate' column to datetime format without timezone information.
        - Converting 'value' column to numeric.
        """
        # Filter and map calorie types
        self.filtered_records_df = self.filtered_records_df[self.filtered_records_df['type'].isin(self.calories_mapping.keys())]
        self.filtered_records_df['type'] = self.filtered_records_df['type'].replace(self.calories_mapping)
        self.filtered_records_df['startDate'] = pd.to_datetime(self.filtered_records_df['startDate']).dt.tz_localize(None)
        self.filtered_records_df["value"] = pd.to_numeric(self.filtered_records_df['value'])

    def process(self):
        """
        Processes the workout and calorie data from the specified start date.

        Args:
            start_date (str): The start date for filtering the data, in 'YYYY-MM-DD' format.

        Returns:
            pd.DataFrame: A DataFrame containing processed workout data with total active energy burned.
        """
        
        # Filter workouts and calories for the specific start date
        records_df = self.filtered_records_df
        workouts_df = self.filtered_workouts_df
        
        activity_calories = []

        for _, workout in workouts_df.iterrows():
            workout_date = workout['creationDate']
            workout_type = workout['workoutActivityType']

            start_time = workout['startDate']
            end_time = workout['endDate']

            # Filter calories within the workout timeframe
            filtered_calories = records_df[(records_df['startDate'] >= start_time) & (records_df['startDate'] <= end_time)]

            active_calories = filtered_calories[filtered_calories['type'] == 'Active Energy Burned']['value'].sum().round(1)

            # Collect unique source names as a list
            source_names = filtered_calories['sourceName'].unique().tolist()

            activity_calories.append({
                'userName': self.user_name,
                'valueGeneratedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'sourceName': ', '.join(source_names),
                'creationDate': workout_date,
                'startDate': start_time,
                'endDate': end_time,
                'workoutActivityType': workout_type,
                'unit': 'min-kcal',
                'duration': workout['Duration (Minutes)'],
                'activeEnergyBurned': active_calories
            })

        final_df = pd.DataFrame(activity_calories)
        final_df = final_df.sort_values(by=['creationDate', 'unit'], ascending=False, ignore_index=True)    

        return final_df