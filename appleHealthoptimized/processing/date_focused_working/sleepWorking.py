import pandas as pd

# Parse XML
from data_source.parse_xml.parseXML import parseXML

# Sleep Pillar
from processing.pillars.sleep.dataStream.s_typeSleep import SSleepType
from processing.pillars.sleep.dataAggregate.s_typeSleep_aggFunc import SSleepTypeAgg
from processing.pillars.sleep.dataAggregate.s_effectiveness_aggFunc import SEffectivenessAgg
from processing.pillars.sleep.dataAggregate.s_efficiency_aggFunc import SEfficiencyAgg
from processing.pillars.sleep.dataAggregate.s_waso_aggFunc import SWASOAgg
from processing.pillars.sleep.dataAggregate.s_onsetLatency_aggFunc import SOnsetLatencyAgg

class SleepDataProcessor:
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
        file_name = f"{file_path}/{remark}_sleep_data.xlsx"
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for sheet_name, df in self.processed_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Sleep Data saved to {file_name}")

    def process_data(self):
        # Process each sleep type with additional arguments
        sleepTypeStream_df = SSleepType(self.records_df, *self.args).process()
        sleepTypeAgg_df = SSleepTypeAgg(self.records_df, *self.args).process()
        effectiveness_df = SEffectivenessAgg(self.records_df, *self.args).process()
        onset_latency_df = SOnsetLatencyAgg(self.records_df, *self.args).process()
        efficiency_df = SEfficiencyAgg(self.records_df, *self.args).process()
        waso_df = SWASOAgg(self.records_df, *self.args).process()

        # Store the processed data in a dictionary
        self.processed_data = {
            'S_SleepType_Stream': sleepTypeStream_df,
            'S_SleepType_Agg': sleepTypeAgg_df,
            'S_Effectiveness_Agg': effectiveness_df,
            'S_OnsetLatency_Agg': onset_latency_df,
            'S_Efficiency_Agg': efficiency_df,
            'S_WASO_Agg': waso_df
        }
