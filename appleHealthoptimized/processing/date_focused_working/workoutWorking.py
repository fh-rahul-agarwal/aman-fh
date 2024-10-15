import pandas as pd

# Parse XML
from data_source.parse_xml.parseXML import parseXML

# Workout Pillar
from processing.pillars.workout.dataStream.w_typeDuration import *
from processing.pillars.workout.dataStream.w_typeCaloriesBurned import *
from processing.pillars.workout.dataStream.w_typeHRQuartile import *
from processing.pillars.workout.dataStream.w_typeVO2Max import *
from processing.pillars.workout.dataStream.w_typePrePostHRQuartile import *

class WorkoutDataProcessor:
    def __init__(self, xml_path, user_name, *args):
        self.args = args
        self.user_name = user_name
        self.xml_path = xml_path
        self.processed_data = {}  # To store processed DataFrames
        self.parsingXML()

    def parsingXML(self):
        parser = parseXML(self.xml_path)
        parsed_data = parser.get_parsed_data()

        self.records_df = parsed_data["records_df"].copy()
        self.workouts_df = parsed_data["workouts_df"].copy()

        # Add the userName column
        self.records_df['userName'] = self.user_name
        self.workouts_df['userName'] = self.user_name

        # Reorder the columns to position 'userName' as the first column
        records_columns = ['userName'] + [col for col in self.records_df.columns if col != 'userName']
        workouts_columns = ['userName'] + [col for col in self.workouts_df.columns if col != 'userName']

        self.records_df = self.records_df[records_columns]
        self.workouts_df = self.workouts_df[workouts_columns]     

    def print_output(self):
        # Print each DataFrame
        self.process_data()
        for sheet_name, df in self.processed_data.items():
            print(f"--- {sheet_name} ---")
            print(df)

    def save_to_excel(self, file_path, remark):
        self.process_data()
        def remove_timezone(df):
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None)
            return df

        # Remove timezone from the DataFrames
        for key in self.processed_data:
            self.processed_data[key] = remove_timezone(self.processed_data[key])

        # Save to Excel
        file_name = f"{file_path}/{remark}_workout_data.xlsx"
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for sheet_name, df in self.processed_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Workout Data saved to {file_name}")

    def process_data(self):
        w_cal_df = WCalories(self.records_df, self.workouts_df, *self.args).process()
        w_pphrq_df = WPrePostHRQuartile(self.records_df, self.workouts_df, *self.args).process()
        w_hrq_df = WHeartRateQuartile(self.records_df, self.workouts_df, *self.args).process()
        w_duration_df = WDuration(self.workouts_df, *self.args).process()
        w_vo2_df = WVO2Max(self.records_df, *self.args).process()

        # Store the processed data in a dictionary
        self.processed_data = {
            'W_WorkoutDuration_Stream': w_duration_df,
            'W_VO2Max_Stream': w_vo2_df,
            'W_WorkoutCalories_Stream': w_cal_df,
            'W_HRQuartile_Stream': w_hrq_df,
            'W_PrePostHRQuartile_Stream': w_pphrq_df
        }