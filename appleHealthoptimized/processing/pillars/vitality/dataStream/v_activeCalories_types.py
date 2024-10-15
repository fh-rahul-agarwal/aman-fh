import pandas as pd

from processing.typeSegmentation.activeEnergyBurned_types import ActiveEnergyBurnedTypeDivisionProcessor

class VActiveCaloriesBurned:
    @staticmethod
    def process(records_df, workouts_df, *args):
        # Instantiate the ActiveEnergyBurnedTypeDivisionProcessor with provided arguments
        processor = ActiveEnergyBurnedTypeDivisionProcessor(records_df, workouts_df, *args)
        
        # Call the process_data method and return the resulting DataFrame
        result_df = processor.process_data()
        return result_df