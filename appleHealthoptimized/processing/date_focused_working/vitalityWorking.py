# Parse XML
import pandas as pd
from data_source.parse_xml.parseXML import parseXML

# Vitality Pillar imports
from processing.pillars.vitality.dataStream.v_respiratoryRate_types import VRespiratoryRate
from processing.pillars.vitality.dataAggregate.v_respiratoryRate_aggFunc import VRespiratoryRateagg
from processing.pillars.vitality.dataStream.v_hr_types import VHeartRate
from processing.pillars.vitality.dataAggregate.v_hr_aggFunc import VHRagg
from processing.pillars.vitality.dataStream.v_hrv_types import VHRV
from processing.pillars.vitality.dataAggregate.v_hrv_aggFunc import VHRVagg
from processing.pillars.vitality.dataStream.v_activeCalories_types import VActiveCaloriesBurned
from processing.pillars.vitality.dataStream.v_restingCalories_types import VRestingCaloriesBurned
from processing.pillars.vitality.dataAggregate.v_totalCalories_aggFunc import VTotalCaloriesagg

class VitalityDataProcessor:
    def __init__(self, xml_path, user_name, *args):
        self.args = args
        self.user_name = user_name
        self.xml_path = xml_path
        self.processed_data = {}  # To store processed DataFrames
        self.records_df, self.workouts_df = self.parsingXML()  # Load data during initialization

    def parsingXML(self):
        parser = parseXML(self.xml_path)
        parsed_data = parser.get_parsed_data()
        records_df = parsed_data["records_df"].copy()
        workouts_df = parsed_data["workouts_df"].copy()

        # Add the userName column and reorder columns to position 'userName' as the first column
        for df in (records_df, workouts_df):
            df['userName'] = self.user_name
            columns = ['userName'] + [col for col in df.columns if col != 'userName']
            df = df[columns]
        # records_df.to_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv', index=False);workouts_df.to_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv', index=False)
        return records_df, workouts_df
    def save_to_excel(self, file_path, remark):
        self.process_data()
        def remove_timezone(df):
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None)
            return df
        self.processed_data = {key: remove_timezone(value) for key, value in self.processed_data.items()}
        file_name = f"{file_path}/{remark}_vitality_data.xlsx"
        
        # Save to Excel without timezone
        with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
            for sheet_name, df in self.processed_data.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        print(f"Vitality Data saved to {file_name}")
    
    # def print_output(self):
    # # Print each DataFrame
    #     self.process_data()
    #     for sheet_name, df in self.processed_data.items():
    #         print(f"--- {sheet_name} ---")
    #         print(df)

    def process_data(self):
        self.processed_data = {
               'V_HR_Continous': VHeartRate().process(self.records_df, self.workouts_df, *self.args),
            'V_HR_Agg': VHRagg(self.records_df, self.workouts_df, *self.args).process_data(),
            'V_HRV_Continous': VHRV().process(self.records_df, self.workouts_df, *self.args),
            'V_HRV_Agg': VHRVagg(self.records_df, self.workouts_df, *self.args).process_data(),
            'V_RespiratoryRate_Continous': VRespiratoryRate().process(self.records_df, self.workouts_df, *self.args),
            'V_RespiratoryRate_Agg': VRespiratoryRateagg(self.records_df, self.workouts_df, *self.args).process_data(),
            'V_ActiveCal_Continous': VActiveCaloriesBurned().process(self.records_df, self.workouts_df, *self.args),
            'V_RestingCal_Continous': VRestingCaloriesBurned().process(self.records_df, self.workouts_df, *self.args),
            'V_TotalCal_Agg': VTotalCaloriesagg(self.records_df, self.workouts_df, *self.args).process_data()
        }


