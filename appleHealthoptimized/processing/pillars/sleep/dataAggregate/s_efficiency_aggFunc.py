import pandas as pd
from processing.pillars.sleep.dataAggregate.s_typeSleep_aggFunc import SSleepTypeAgg

class SEfficiencyAgg:
    def __init__(self, records_df: pd.DataFrame, *args):
        self.records_df = records_df
        self.sleep_data_processor = SSleepTypeAgg(self.records_df, *args)
        self.sleep_data = self.sleep_data_processor.process()
        self.type = self.sleep_data['type'].iloc[0] 
        self.s_name = 'S_Efficiency'
    
    def process(self) -> pd.DataFrame:
        result_df = self.sleep_data

        # Apply calculations
        result_df['date'] = pd.to_datetime(result_df['date']).dt.date
        result_df = result_df.groupby(['userName', 'valueGeneratedAt', 'date']).apply(self.calculate_efficiency).reset_index()

        # Prepare the final DataFrame in the desired format
        final_df = pd.DataFrame()

        # Add Efficiency information
        final_df = pd.concat([final_df, self.create_rows(result_df, 'SleepEfficiency', 'efficiency', '%')])

        final_df = final_df.sort_values(by=['date', 'unit'], ascending=False, ignore_index=True)
        final_df['type'] = self.type
        final_df['s_name'] = self.s_name
        
        return final_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]

    @staticmethod
    def calculate_efficiency(df: pd.DataFrame) -> pd.Series:
        total_sleep_duration = df[df['valueType'] == 'TotalSleepDuration']['value'].sum()
        in_bed_duration = df[df['valueType'] == 'TotalInBedDuration']['value'].sum()
        efficiency = (total_sleep_duration / in_bed_duration * 100) if in_bed_duration > 0 else pd.NA
        efficiency_rounded = round(efficiency, 1) if not pd.isna(efficiency) else pd.NA
        return pd.Series({
            'efficiency': efficiency_rounded,
            'TotalSleepDuration': total_sleep_duration,
            'TotalInBedDuration': in_bed_duration
        })

    @staticmethod
    def create_rows(df: pd.DataFrame, value_type: str, value_col: str, unit: str) -> pd.DataFrame:
        """
        Helper function to create rows for each valueType.
        It will generate a DataFrame with corresponding 'valueType', 'value', and 'unit'.
        """
        return pd.DataFrame({
            'userName': df['userName'],
            'valueGeneratedAt': df['valueGeneratedAt'],
            'date': df['date'],
            'unit': unit,        # Corresponds to 'unit' column (e.g., %, min)
            'valueType': value_type,  # The type of value (e.g., SleepEfficiency, TotalSleepDuration)
            'value': df[value_col]  # The actual value (e.g., efficiency, totalSleepDuration, inBedDuration)
        })
