import pandas as pd
from datetime import datetime, timedelta
from processing.customLogs import *

logger = setup_logger()
class AFlightsClimbed:
    def __init__(self, records_df, *args):
        try:
            # logger.start('AFlightsClimbed', '__init__', "Initiated")

            self.records_df = records_df[records_df["type"] == 'HKQuantityTypeIdentifierFlightsClimbed'].copy()
            self.records_df['value'] = pd.to_numeric(self.records_df['value'], errors='coerce')
            self.records_df.dropna(subset=['value'], inplace=True)
            self.value_generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Initialize filtered_records_df based on input arguments
            if len(args) == 1 and isinstance(args[0], list):
                dates_list = [pd.to_datetime(date).normalize() for date in args[0]]
                self.filtered_records_df = self._filter_by_dates_list(dates_list)
            elif len(args) == 1:
                start_date = pd.to_datetime(args[0]).normalize()
                self.filtered_records_df = self._filter_by_single_date(start_date)
            elif len(args) == 2:
                start_date, end_date = pd.to_datetime(args[0]).normalize(), pd.to_datetime(args[1]).normalize()
                self.filtered_records_df = self._filter_by_date_range(start_date, end_date)
            elif len(args) == 3:
                start_date, days_offset, offset_sign = pd.to_datetime(args[0]).normalize(), int(args[1]), args[2]
                self.filtered_records_df = self._filter_by_offset(start_date, days_offset, offset_sign)

            # Handle empty case
            if self.filtered_records_df.empty:
                self.filtered_records_df = self._handle_empty_records()

            # logger.success('AFlightsClimbed', '__init__', "Completed")

        except Exception as e:
            logger.error('AFlightsClimbed', '__init__', f"Error during initialization: {str(e)}")

    def _filter_by_single_date(self, start_date):
        # logger.start('AFlightsClimbed', '_filter_by_single_date', "Initiated")
        try:
            result = self._filter_data(start_date)
            # logger.success('AFlightsClimbed', '_filter_by_single_date', "Completed")
            return result
        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_by_single_date', f"Error during filtering by single date: {str(e)}")

    def _filter_by_date_range(self, start_date, end_date):
        # logger.start('AFlightsClimbed', '_filter_by_date_range', "Initiated")
        try:
            result = self._filter_data(start_date, end_date)
            # logger.success('AFlightsClimbed', '_filter_by_date_range', "Completed")
            return result
        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_by_date_range', f"Error during filtering by date range: {str(e)}")

    def _filter_by_offset(self, start_date, days_offset, offset_sign):
        # logger.start('AFlightsClimbed', '_filter_by_offset', "Initiated")
        try:
            if offset_sign == '+':
                end_date = start_date + timedelta(days=days_offset)
            else:
                end_date = start_date
                start_date = start_date - timedelta(days=days_offset)
            result = self._filter_data(start_date, end_date)
            # logger.success('AFlightsClimbed', '_filter_by_offset', "Completed")
            return result
        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_by_offset', f"Error during filtering by offset: {str(e)}")

    def _filter_by_dates_list(self, dates_list):
        # logger.start('AFlightsClimbed', '_filter_by_dates_list', "Initiated")
        try:
            start_dates = pd.to_datetime(dates_list).normalize()
            filtered_df = self.records_df[self.records_df['startDate'].dt.normalize().isin(start_dates)].copy()
            # logger.success('AFlightsClimbed', '_filter_by_dates_list', "Completed")
            return filtered_df
        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_by_dates_list', f"Error during filtering by dates list: {str(e)}")

    def _filter_data(self, start_date, end_date=None):
        # logger.start('AFlightsClimbed', '_filter_data', "Initiated")
        try:
            if self.records_df.empty:
                logger.cwarning('AFlightsClimbed', '_filter_data', "Empty DF")
                return pd.DataFrame(columns=self.records_df.columns)

            df = self.records_df.copy()
            df['startDate'] = pd.to_datetime(df['startDate']).dt.tz_localize(None)
            df['endDate'] = pd.to_datetime(df['endDate']).dt.tz_localize(None)

            start_of_day = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = start_of_day.replace(hour=23, minute=59, second=59, microsecond=999999) if not end_date else end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            filtered_df = df[(df['startDate'] >= start_of_day) & (df['startDate'] <= end_of_day) & (df['endDate'] <= end_of_day)]

            # logger.success('AFlightsClimbed', '_filter_data', "Completed")
            return filtered_df.reset_index(drop=True)
        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_data', f"Error during filtering data: {str(e)}")

    def _handle_empty_records(self):
        # logger.start('AFlightsClimbed', '_handle_empty_records', "Initiated")
        try:
            logger.cwarning('AFlightsClimbed', '_handle_empty_records', "No Flights Records Found")
            return pd.DataFrame(columns=['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'creationDate', 'startDate', 
                         'endDate', 'unit', 'value'])
        except Exception as e:
            logger.error('AFlightsClimbed', '_handle_empty_records', f"Error during handling empty records: {str(e)}")

    def _filter_and_sum_by_source(self, df):
        # logger.start('AFlightsClimbed', '_filter_and_sum_by_source', "Initiated")
        try:
            if df.empty:
                logger.cwarning('AFlightsClimbed', '_filter_and_sum_by_source', "No Records")
                return None, None

            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            grouped_df = df.groupby('sourceName', as_index=False)['value'].sum()
            max_source = grouped_df.loc[grouped_df['value'].idxmax()]
            # logger.success('AFlightsClimbed', '_filter_and_sum_by_source', "Completed")
            return max_source['sourceName'], max_source['value']

        except Exception as e:
            logger.error('AFlightsClimbed', '_filter_and_sum_by_source', f"Error during filtering and summing by source: {str(e)}")

    def process(self):
        # logger.start('AFlightsClimbed', 'process', "Initiated")
        try:
            filtered_by_type = self.filtered_records_df

            if filtered_by_type.empty:
                logger.cwarning('AFlightsClimbed', 'process', "Terminated With No Flights Records Found")
                return pd.DataFrame(columns=['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'creationDate', 'startDate', 
                         'endDate', 'unit', 'value'])

            max_source_name, _ = self._filter_and_sum_by_source(filtered_by_type)

            if max_source_name is None:
                logger.cwarning('AFlightsClimbed', 'process', "No valid source found")
                return pd.DataFrame(columns=['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'creationDate', 'startDate', 
                         'endDate', 'unit', 'value'])

            final_df = filtered_by_type[filtered_by_type['sourceName'] == max_source_name].copy()
            final_df['valueGeneratedAt'] = self.value_generated_at
            final_df['dateSorting'] = final_df['startDate'].dt.date
            final_df = final_df.sort_values(by=['dateSorting', 'type'], ascending=False, ignore_index=True)

            # logger.success('AFlightsClimbed', 'process', "Completed")
            return final_df[['userName', 'valueGeneratedAt', 'type', 'sourceName', 'sourceVersion', 'creationDate', 'startDate', 
                            'endDate', 'unit', 'value']].copy()
        except Exception as e:
            logger.error('AFlightsClimbed', 'process', f"Error during processing: {str(e)}")
