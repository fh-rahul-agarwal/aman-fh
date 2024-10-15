import pandas as pd
from processing.pillars.sleep.dataAggregate.s_typeSleep_aggFunc import SSleepTypeAgg

class SWASOAgg:
    def __init__(self, records_df: pd.DataFrame, *args):
        self.records_df = records_df
        self.sleep_data_processor = SSleepTypeAgg(self.records_df, *args)
        self.sleep_data = self.sleep_data_processor.process()
        self.type = self.sleep_data['type'].iloc[0] 
        self.s_name = 'S_WASO'
    
    def process(self) -> pd.DataFrame:
        self.sleep_data['date'] = pd.to_datetime(self.sleep_data['date']).dt.date
        result_df = self._calculate_waso(self.sleep_data)
        
        final_df = self._create_rows(result_df, 'WakeAfterSleepOnset', 'waso', '%')

        final_df = final_df.sort_values(by=['date', 'valueType'], ascending=False, ignore_index=True)
        final_df['type'] = self.type
        final_df['s_name'] = self.s_name

        return final_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]

    def _calculate_waso(self, df: pd.DataFrame) -> pd.DataFrame:
        waso_df = df.groupby(['userName', 'valueGeneratedAt', 'date']).apply(self._waso_formula).reset_index()
        return waso_df

    @staticmethod
    def _waso_formula(group: pd.DataFrame) -> pd.Series:
        group = group.dropna(subset=['valueType', 'value'])
        awake_duration = group[group['valueType'].str.contains('Awake')]['value'].sum()
        total_sleep_duration = group[group['valueType'] == 'TotalSleepDuration']['value'].sum()

        if total_sleep_duration > 0:
            waso = round((awake_duration / total_sleep_duration) * 100, 1)
        else:
            waso = pd.NA

        return pd.Series({
            'waso': waso,
            'awakeDuration': awake_duration,
            'totalSleepDuration': total_sleep_duration
        })

    @staticmethod
    def _create_rows(df: pd.DataFrame, value_type: str, value_col: str, unit: str) -> pd.DataFrame:
        return pd.DataFrame({
            'userName': df['userName'],
            'valueGeneratedAt': df['valueGeneratedAt'],
            'date': df['date'],
            'unit': unit,  # The unit of measurement (e.g., %, min)
            'valueType': value_type,  # The type of value (e.g., AwakeDuration, TotalSleepDuration)
            'value': df[value_col]  # The actual value (e.g., waso, awakeDuration, totalSleepDuration)
        })
