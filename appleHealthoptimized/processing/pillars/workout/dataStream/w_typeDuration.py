import pandas as pd
from datetime import datetime, timedelta

class WDuration:

    def __init__(self, workouts_df, *args):
        self.workouts_df = workouts_df
        
        # Convert columns to datetime
        self.workouts_df["startDate"] = pd.to_datetime(self.workouts_df["startDate"]).dt.tz_localize(None)
        self.workouts_df["endDate"] = pd.to_datetime(self.workouts_df["endDate"]).dt.tz_localize(None)
        self.workouts_df["creationDate"] = pd.to_datetime(self.workouts_df["creationDate"]).dt.tz_localize(None)
        self.workouts_df["Duration (Minutes)"] = (self.workouts_df["endDate"] - self.workouts_df["startDate"]).dt.total_seconds() / 60
        self.workouts_df["Duration (Minutes)"] = self.workouts_df["Duration (Minutes)"].round(3)

        # Filter data based on arguments
        if len(args) == 1 and isinstance(args[0], list):
            self.dates_list = [pd.to_datetime(date).tz_localize(None) for date in args[0]]
            self.filtered_workouts_df = self._filter_by_dates_list(self.workouts_df, self.dates_list)
        elif len(args) == 1:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.filtered_workouts_df = self._filter_by_single_date(self.workouts_df, self.start_date)
        elif len(args) == 2:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.end_date = pd.to_datetime(args[1]).tz_localize(None)
            self.filtered_workouts_df = self._filter_by_date_range(self.workouts_df, self.start_date, self.end_date)
        elif len(args) == 3:
            self.start_date = pd.to_datetime(args[0]).tz_localize(None)
            self.days_offset = int(args[1])
            self.offset_sign = args[2]
            self.filtered_workouts_df = self._filter_by_offset(self.workouts_df, self.start_date, self.days_offset, self.offset_sign)

        if self.filtered_workouts_df.empty:
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
            return pd.DataFrame(columns=['userName', 'workoutActivityType', 'startDate', 'endDate', 'creationDate', 'Duration (Minutes)'])
        
        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999) if end_date else start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)

        # Use boolean indexing for filtering
        filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day)]
        
        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        print("No workout data available for the specified dates.")

    def process(self):
        if self.filtered_workouts_df.empty:
            print("No data available for processing.")
            return pd.DataFrame(columns=[
                'userName', 'valueGeneratedAt', 'sourceName', 'creationDate', 'startDate', 'endDate', 'workoutActivityType', 'unit', 'value'
            ])

        self.filtered_workouts_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.filtered_workouts_df['unit'] = 'min'

        final_workout_df = self.filtered_workouts_df.rename(columns={'Duration (Minutes)': 'value'})

        final_workout_df['startDate'] = pd.to_datetime(final_workout_df['startDate'])
        final_workout_df['dateSorting'] = final_workout_df['startDate'].dt.date
        final_workout_df = final_workout_df.sort_values(by=['dateSorting', 'unit'], ascending=False, ignore_index=True)

        final_workout_df = final_workout_df[['userName', 'valueGeneratedAt', 'sourceName', 'creationDate', 'startDate', 'endDate', 'workoutActivityType', 'unit', 'value']]
        final_workout_df['value'] = final_workout_df['value'].round(1)

        return final_workout_df
