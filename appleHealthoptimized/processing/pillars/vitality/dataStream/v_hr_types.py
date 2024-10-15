from processing.typeSegmentation.heartRate_types import HeartRateTypeDivisionProcessor

class VHeartRate:
    @staticmethod
    def process(records_df, workouts_df, *args):
        # Instantiate the heartRateTypeDivisionProcessor with provided arguments
        processor = HeartRateTypeDivisionProcessor(records_df, workouts_df, *args)
        
        # Call the process_data method and return the resulting DataFrame
        result_df = processor.process_data()
        return result_df
    

# import sys
# import importlib.util
# import os

# # Specify the path to the directory containing the module
# # For example, if 'typeSegmentation' is in 'C:\Users\amank\Downloads\Fithack\Optimization\processing'
# module_path = os.path.abspath(r'C:\Users\amank\Downloads\Fithack\Optimization\processing\typeSegmentation\heartRate_types.py')

# # Define the module name (this can be anything you want to refer to this module as)
# module_name = 'heartRate_types'

# spec = importlib.util.spec_from_file_location(module_name, module_path)
# module = importlib.util.module_from_spec(spec)
# sys.modules[module_name] = module
# spec.loader.exec_module(module)

# # Now you can use HeartRateTypeDivisionProcessor from the manually imported module
# HeartRateTypeDivisionProcessor = module.HeartRateTypeDivisionProcessor

# records_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\records_df.csv')
# workouts_df = pd.read_csv(r'C:\Users\amank\Downloads\Fithack\Optimization\local_files\workouts_df.csv')

# args = ['2024-09-13', 10, '-']

# result_df = VHeartRate.process(records_df, workouts_df, *args) 
# # result_df = processor.process()
# print(result_df)