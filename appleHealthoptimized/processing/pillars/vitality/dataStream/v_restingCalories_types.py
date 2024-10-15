import pandas as pd

from processing.typeSegmentation.restingEnergyBurned_types import RestingEnergyBurnedTypeDivisionProcessor

class VRestingCaloriesBurned:
    @staticmethod
    def process(records_df, workouts_df, *args):
        # Instantiate the restingEnergyBurnedTypeDivisionProcessor with provided arguments
        processor = RestingEnergyBurnedTypeDivisionProcessor(records_df, workouts_df, *args)
        
        # Call the process_data method and return the resulting DataFrame
        result_df = processor.process_data()
        return result_df