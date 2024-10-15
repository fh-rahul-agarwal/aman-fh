import pandas as pd

#Parse XML
from data_source.parse_xml.parseXML import *

#Activity Pillar
from processing.pillars.activity.dataAggregate.a_standTime_aggFunc import AStandTimeAgg
from processing.pillars.activity.dataAggregate.a_stepCount_aggFunc import AStepCountAgg
from processing.pillars.activity.dataAggregate.a_walkingRunningDistance_aggFunc import AWalkingRunningDistanceAgg
from processing.pillars.activity.dataAggregate.a_flightsClimbed_aggFunc import AFlightsClimbedAgg
from processing.pillars.activity.dataAggregate.a_activityCalories_aggFunc import AActivityCaloriesAgg

from processing.pillars.activity.dataStream.a_standTime import AStandTime
from processing.pillars.activity.dataStream.a_stepCount import AStepCount
from processing.pillars.activity.dataStream.a_walkingRunningDistance import AWalkingRunningDistance
from processing.pillars.activity.dataStream.a_flightsClimbed import AFlightsClimbed
from processing.pillars.activity.dataStream.a_activityCalories import AActivityCalories

class ActivityDataProcessor:
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

    # def print_output(self):
    #     # Print each DataFrame
    #     self.process_data()
    #     for sheet_name, df in self.processed_data.items():
    #         print(f"--- {sheet_name} ---")
    #         print(df)

    def save_to_excel(self, file_path, remark):
        self.process_data()
        def remove_timezone(df):
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None)
            return df

        self.processed_data = {key: remove_timezone(value) for key, value in self.processed_data.items()}

        # Save to Excel
        file_name = f"{file_path}/{remark}_activity_data.xlsx"
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for sheet_name, df in self.processed_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Activity Data saved to {file_name}")


    def process_data(self):
        # Process each activity type
        fc_df = AFlightsClimbed(self.records_df, *self.args).process()
        fca_df = AFlightsClimbedAgg(self.records_df, *self.args).process()

        wrd_df = AWalkingRunningDistance(self.records_df, *self.args).process()
        wrda_df = AWalkingRunningDistanceAgg(self.records_df, *self.args).process()

        sc_df = AStepCount(self.records_df, *self.args).process()
        sca_df = AStepCountAgg(self.records_df, *self.args).process()

        st_df = AStandTime(self.records_df, *self.args).process()
        sta_df = AStandTimeAgg(self.records_df, *self.args).process()

        ac_df = AActivityCalories(self.records_df, self.workouts_df, *self.args).process()
        aca_df = AActivityCaloriesAgg(self.records_df, self.workouts_df, *self.args).process()

        # Store the processed data in a dictionary
        self.processed_data = {
            'A_StandTime_Stream': st_df,
            'A_StandTime_Agg': sta_df,
            'A_FlightsClimbed_Stream': fc_df,
            'A_FlightsClimbed_Agg': fca_df,
            'A_StepCount_Stream': sc_df,
            'A_StepCount_Agg': sca_df,
            'A_WalkingRunningDistance_Stream': wrd_df,
            'A_WalkingRunningDistance_Agg': wrda_df,
            'A_ActivityCalories_Stream': ac_df,
            'A_ActivityCalories_Agg': aca_df
        }