import xml.etree.ElementTree as ET
import pandas as pd

class XMLDataExtractor:
    def __init__(self, file_path, specific_child_tag):
        self.file_path = file_path
        self.specific_child_tag = specific_child_tag
        self.root = self.read_xml()

    def read_xml(self):
        """Reads an XML file and returns the root element."""
        return ET.parse(self.file_path).getroot()

    def extract_base_data(self, element):
        """Extracts base attributes from an element."""
        return element.attrib.copy()

    def extract_metadata_entries(self, element, base_data):
        """Extracts metadata entries and adds them to the base data."""
        for metadata_entry in element.findall('.//MetadataEntry'):
            base_data[f'MetadataEntry_{metadata_entry.attrib["key"]}'] = metadata_entry.attrib.get('value')
        return base_data

    def process_nested_element(self, nested_element, base_data):
        """Processes a nested element and extracts its attributes."""
        data = base_data.copy()
        data['element_type'] = nested_element.tag
        
        for attr_key, attr_value in nested_element.attrib.items():
            data[f'{nested_element.tag}_{attr_key}'] = attr_value
        
        return data

    def process_sub_element(self, sub_element, data):
        """Processes sub-elements and extracts their attributes."""
        sub_element_data = data.copy()
        sub_element_data['sub_element_type'] = sub_element.tag
        for sub_attr_key, sub_attr_value in sub_element.attrib.items():
            sub_element_data[f'{sub_element.tag}_{sub_attr_key}'] = sub_attr_value
        return sub_element_data

    def extract_data(self):
        """Extracts data from the specific child elements and returns a DataFrame."""
        child_dataframes = []
        for child in self.root.findall(self.specific_child_tag):
            records_data = []
            base_data = self.extract_base_data(child)

            # Extract metadata entries if no child elements exist
            if not child:
                base_data = self.extract_metadata_entries(child, base_data)
                records_data.append(base_data)

            # Process metadata entries at the current level
            base_data = self.extract_metadata_entries(child, base_data)

            # Process nested elements and their sub-elements
            for nested_element in child:
                data = self.process_nested_element(nested_element, base_data)
                
                for sub_element in nested_element:
                    sub_element_data = self.process_sub_element(sub_element, data)
                    records_data.append(sub_element_data)
                
                # Add data if the nested element has no sub-elements
                if not nested_element:
                    records_data.append(data)

            if records_data:
                child_dataframes.append(pd.DataFrame(records_data))
        
        # Concatenate all the extracted DataFrames for the specific child tag
        if child_dataframes:
            return pd.concat(child_dataframes, ignore_index=True)
        else:
            return pd.DataFrame()

# Usage
# file_path = r'/content/export.xml'
# specific_child_tag = 'Record'
# extractor = XMLDataExtractor(file_path, specific_child_tag)
# record_df = extractor.extract_data()
