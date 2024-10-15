import pandas as pd
from datetime import datetime, timedelta

class WVO2Max:

    def __init__(self, records_df, *args):
        self.records_df = records_df[records_df["type"] == 'HKQuantityTypeIdentifierVO2Max'].copy()
        self.records_df['creationDate'] = pd.to_datetime(self.records_df['creationDate']).dt.tz_localize(None)
        self.records_df['value'] = pd.to_numeric(self.records_df['value'], errors='coerce')
        self.records_df = self.records_df.dropna(subset=['value'])

        # Filter data based on arguments
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
            return pd.DataFrame(columns=['userName', 'creationDate', 'value', 'type'])
        
        start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        if end_date:
            end_of_day = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['creationDate'] >= start_of_day) & (df['creationDate'] <= end_of_day)].copy()
        else:
            end_of_day = start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['creationDate'] >= start_of_day) & (df['creationDate'] <= end_of_day)].copy()
        
        return filtered_df.reset_index(drop=True)

    def _handle_empty_records(self):
        print("No VO2 Max data available for the specified dates.")

    def process(self):
        if self.filtered_records_df.empty:
            print("No data available for processing.")
            return pd.DataFrame(columns=[
                'userName', 'valueGeneratedAt', 'sourceName', 'creationDate', 'startDate', 'endDate', 'type', 'unit', 'value'
            ])

        # Add user name, generation time
        self.filtered_records_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.filtered_records_df['unit'] = self.filtered_records_df['unit'].fillna('unit')  # Assuming 'unit' could be missing

        # Reorder columns and sort by date
        final_vo2max_df = self.filtered_records_df[['userName', 'valueGeneratedAt', 'sourceName', 'creationDate', 'startDate', 'endDate', 'type', 'unit', 'value']]
        final_vo2max_df = final_vo2max_df.sort_values(by=['creationDate'], ascending=False, ignore_index=True)
        final_vo2max_df['value'] = final_vo2max_df['value'].round(2)

        return final_vo2max_df
