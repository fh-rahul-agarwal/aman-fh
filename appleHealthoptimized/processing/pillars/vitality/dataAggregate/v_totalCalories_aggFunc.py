import pandas as pd
from datetime import datetime

from processing.typeSegmentation.activeEnergyBurned_types import ActiveEnergyBurnedTypeDivisionProcessor
from processing.typeSegmentation.restingEnergyBurned_types import RestingEnergyBurnedTypeDivisionProcessor

class VTotalCaloriesagg:

    def __init__(self, records_df, workouts_df, *args):
        self.records_df = records_df
        self.workouts_df = workouts_df

        # Instantiate and process using ActiveEnergyBurnedTypeDivisionProcessor
        self.active_processor = ActiveEnergyBurnedTypeDivisionProcessor(self.records_df, self.workouts_df, *args)
        self.active_df = self.active_processor.process_data()
        self.active_type = self.active_df['type'].iloc[0]

        # Instantiate and process using RestingEnergyBurnedTypeDivisionProcessor
        self.resting_processor = RestingEnergyBurnedTypeDivisionProcessor(self.records_df, self.workouts_df, *args)
        self.resting_df = self.resting_processor.process_data()
        self.resting_type = self.resting_df['type'].iloc[0]
        self.s_name = 'V_TotalCalories'

    def map_type(self, valueType):
        if valueType == 'RestingCaloriesBurned':
            return self.resting_type
        elif valueType == 'ActiveCaloriesBurned':
            return self.active_type
        else:
            return pd.NA  
        
    def process_data(self):

        active_df = self.active_df
        resting_df = self.resting_df

        # Convert 'value' column to numeric
        active_df['value'] = pd.to_numeric(active_df['value'], errors='coerce')
        resting_df['value'] = pd.to_numeric(resting_df['value'], errors='coerce')

        # Combine active and resting data
        combined_df = pd.concat([active_df, resting_df], ignore_index=True)

        # Convert 'startDate' to datetime and extract date
        combined_df['startDate'] = pd.to_datetime(combined_df['startDate'])
        combined_df['date'] = combined_df['startDate'].dt.date
        combined_df['valueType'] = combined_df['type']

        # Aggregate data by date and type
        aggregated_df = combined_df.groupby(['userName', 'date', 'unit', 'valueType']).agg({'value': 'sum'}).reset_index()

        # Create totalCaloriesBurned for each date
        total_calories_df = aggregated_df.groupby(['userName', 'date', 'unit']).agg({'value': 'sum'}).reset_index()
        total_calories_df['valueType'] = 'TotalCaloriesBurned'

        # Concatenate totalCaloriesBurned with aggregated data
        final_df = pd.concat([aggregated_df, total_calories_df], ignore_index=True)

        final_df['valueType'] = final_df['valueType'].replace({
            'HKQuantityTypeIdentifierBasalEnergyBurned': 'RestingCaloriesBurned',
            'HKQuantityTypeIdentifierActiveEnergyBurned': 'ActiveCaloriesBurned'
        })

        # Add valueGeneratedAt column
        final_df['valueGeneratedAt'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Round the 'value' column to 1 decimal place
        final_df['value'] = final_df['value'].round(1)

        final_df['type'] = final_df['valueType'].apply(self.map_type)
        final_df['s_name'] = self.s_name

        # Reorder columns
        final_df = final_df[['userName', 'valueGeneratedAt', 's_name', 'type', 'date', 'unit', 'valueType', 'value']]

        # Sort by date and type
        final_df = final_df.sort_values(by=['date', 'valueType'], ascending=False).reset_index(drop=True)

        return final_df
