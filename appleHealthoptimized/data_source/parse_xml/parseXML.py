import pandas as pd
from lxml import etree

class parseXML:
    _instance = None
    _parsed_data = None

    def __new__(cls, file_path):
        if cls._instance is None or cls._instance.file_path != file_path:
            cls._instance = super(parseXML, cls).__new__(cls)
            cls._instance.file_path = file_path
            cls._instance._parsed_data = None
            cls._instance.root = cls._instance._parse_xml()
        return cls._instance

    def _parse_xml(self):
        """
        Parse the XML file and return the root element.

        :return: lxml.etree.Element, root of the XML tree
        """
        try:
            parser = etree.XMLParser(recover=True)
            tree = etree.parse(self.file_path, parser)
            return tree.getroot()
        except etree.XMLSyntaxError as e:
            print(f"XML Syntax Error: {e}")
            return None

    def _extract_data(self, tag):
        """
        Extract data from XML based on the specified tag.

        :param tag: str, tag name to extract data from
        :return: pd.DataFrame, DataFrame containing the extracted data
        """
        elements = [dict(child.attrib) for child in self.root if child.tag == tag]
        return pd.DataFrame(elements) if elements else pd.DataFrame()

    def get_parsed_data(self):
        """
        Parse the XML and return a dictionary with records, workouts, and activity summaries data.
        If the data has already been parsed, return the cached data.

        :return: dict, containing DataFrames for 'records_df', 'workouts_df', and 'activity_summaries_df'
        """
        if self._parsed_data is None:
            self._parsed_data = {
                "records_df": self._extract_data('Record'),
                "workouts_df": self._extract_data('Workout'),
                "activity_summaries_df": self._extract_data('ActivitySummary')
            }
        return self._parsed_data
