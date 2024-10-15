import pandas as pd

from processing.typeSegmentation.hrv_types import HRVTypeDivisionProcessor

class VHRV:
    @staticmethod
    def process(records_df, workouts_df, *args):
        # Instantiate the HRVTypeDivisionProcessor with provided arguments
        processor = HRVTypeDivisionProcessor(records_df, workouts_df, *args)
        
        # Call the process_data method and return the resulting DataFrame
        result_df = processor.process_data()
        return result_df