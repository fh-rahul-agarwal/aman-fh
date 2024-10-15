#All Pillars Working
from processing.date_focused_working.vitalityWorking import VitalityDataProcessor
from processing.date_focused_working.activityWorking import ActivityDataProcessor
from processing.date_focused_working.sleepWorking import SleepDataProcessor
from processing.date_focused_working.workoutWorking import WorkoutDataProcessor

class PillarsDataProcessor:

    def __init__(self, xml_path, user_name, *args):
        self.xml_path = xml_path
        self.args = args
        self.user_name = user_name

    def process(self):
        self.v_data = VitalityDataProcessor(self.xml_path, self.user_name, *self.args)
        self.a_data = ActivityDataProcessor(self.xml_path, self.user_name, *self.args)
        self.s_data = SleepDataProcessor(self.xml_path, self.user_name, *self.args)
        self.w_data = WorkoutDataProcessor(self.xml_path, self.user_name, *self.args)

    def saveExcel(self, file_path, remark):
        self.process()
        self.v_data.save_to_excel(file_path, remark)
        self.a_data.save_to_excel(file_path, remark)
        self.s_data.save_to_excel(file_path, remark)
        self.w_data.save_to_excel(file_path, remark)