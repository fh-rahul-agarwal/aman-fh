from processing.typeSegmentation.respiratoryRate_types import RespiratoryRateTypeDivisionProcessor

class VRespiratoryRate:
    @staticmethod
    def process(records_df, workouts_df, *args) :
        # Instantiate the RespiratoryRateTypeDivisionProcessor with provided arguments
        processor = RespiratoryRateTypeDivisionProcessor(records_df, workouts_df, *args)
        
        # Call the process_data method and return the resulting DataFrame
        result_df = processor.process_data()
        return result_df