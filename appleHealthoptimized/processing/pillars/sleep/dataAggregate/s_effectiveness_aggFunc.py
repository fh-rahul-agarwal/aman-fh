import pandas as pd
from datetime import datetime
from processing.pillars.sleep.dataAggregate.s_typeSleep_aggFunc import SSleepTypeAgg
from processing.pillars.sleep.dataAggregate.s_efficiency_aggFunc import SEfficiencyAgg
from processing.pillars.sleep.dataAggregate.s_waso_aggFunc import SWASOAgg
from processing.pillars.sleep.dataAggregate.s_onsetLatency_aggFunc import SOnsetLatencyAgg

class SEffectivenessAgg:
    def __init__(self, records_df: pd.DataFrame, *args):
        self.records_df = records_df
        self.args = args
        self.value_generated_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sleep_data_processor = SSleepTypeAgg(self.records_df, *self.args)
        self.sleep_df = sleep_data_processor.process()
        self.s_name = 'S_Effectiveness'

    def get_sleep_type(self):
        sleep_data = self.sleep_df
        sleep_data_map = ['TotalInBedDuration', 'TotalSleepDuration']
        sleep_data = sleep_data[sleep_data['valueType'].isin(sleep_data_map)].copy()  # Make an explicit copy
        sleep_data.loc[:, 'valueGeneratedAt'] = self.value_generated_at  # Use .loc for setting values
        return sleep_data

    def get_sleep_efficiency(self):
        sleep_efficiency_processor = SEfficiencyAgg(self.records_df, *self.args)
        efficiency_df = sleep_efficiency_processor.process()
        efficiency_df = efficiency_df[['userName', 'date', 'type', 'unit', 'valueType', 'value']]
        efficiency_df.loc[:, 'valueGeneratedAt'] = self.value_generated_at  # Use .loc for setting values
        return efficiency_df

    def get_waso(self):
        waso_processor = SWASOAgg(self.records_df, *self.args)
        waso_df = waso_processor.process()
        waso_df = waso_df[['userName', 'date', 'type', 'unit', 'valueType', 'value']]
        waso_df.loc[:, 'valueGeneratedAt'] = self.value_generated_at  # Use .loc for setting values
        return waso_df

    def get_onset_latency(self):
        onset_latency_processor = SOnsetLatencyAgg(self.records_df, *self.args)
        onset_latency_df = onset_latency_processor.process()
        onset_latency_df = onset_latency_df[['userName', 'date', 'type', 'unit', 'valueType', 'value']]
        onset_latency_df.loc[:, 'valueGeneratedAt'] = self.value_generated_at  # Use .loc for setting values
        return onset_latency_df


    def get_deep_sleep_percentage(self, total_sleep_duration, deep_sleep_duration):
        return round((deep_sleep_duration / total_sleep_duration) * 100, 1)

    def calculate_deep_sleep_percentage(self):
        sleep_df = self.sleep_df
        dftype = sleep_df['type'].iloc[0]
        userName = sleep_df['userName'].iloc[0]
        sleep_df['date'] = pd.to_datetime(sleep_df['date']).dt.date
        
        aggregated_df = sleep_df.groupby(['date', 'valueType']).agg({'value': 'sum'}).reset_index()
        total_sleep_df = aggregated_df[aggregated_df['valueType'] == 'TotalSleepDuration']
        deep_sleep_df = aggregated_df[aggregated_df['valueType'] == 'TotalDeepSleepDuration']
        
        merged_df = pd.merge(total_sleep_df, deep_sleep_df, on='date', how='left', suffixes=('_total', '_deep'))
        
        merged_df['deepSleepPercentage'] = merged_df.apply(
            lambda row: self.get_deep_sleep_percentage(
                row.get('value_total', 0),
                row.get('value_deep', 0)
            ),
            axis=1
        )
        
        deep_sleep_df = merged_df[['date', 'deepSleepPercentage']].copy()
        deep_sleep_df['userName'] = userName
        deep_sleep_df['valueGeneratedAt'] = self.value_generated_at
        deep_sleep_df['type'] = dftype
        deep_sleep_df['unit'] = '%'
        deep_sleep_df['valueType'] = 'DeepSleepPercentage'
        deep_sleep_df = deep_sleep_df.rename(columns={'deepSleepPercentage': 'value'})
        return deep_sleep_df[['userName', 'valueGeneratedAt', 'date', 'type', 'unit', 'valueType', 'value']]

    def process(self) -> pd.DataFrame:
        sleep_data = self.get_sleep_type()
        efficiency_df = self.get_sleep_efficiency()
        waso_df = self.get_waso()
        onset_latency_df = self.get_onset_latency()
        deep_sleep_df = self.calculate_deep_sleep_percentage()

        final_df = pd.concat([sleep_data, efficiency_df, waso_df, onset_latency_df, deep_sleep_df], ignore_index=True)

        required_value_types = [
            'TotalSleepDuration',
            'TotalInBedDuration',
            'DeepSleepPercentage',
            'SleepEfficiency',
            'WakeAfterSleepOnset',
            'SleepOnsetLatency'
        ]

        final_df = final_df[final_df['valueType'].isin(required_value_types)]
        final_df = final_df.sort_values(by=['date', 'type', 'valueType'], ascending=False, ignore_index=True)
        final_df = final_df.drop_duplicates(subset=['date', 'valueType'])
        final_df['s_name'] = self.s_name
        return final_df[['userName', 'valueGeneratedAt', 's_name', 'date', 'type', 'unit', 'valueType', 'value']]
