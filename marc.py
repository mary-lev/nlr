from pymarc import MARCReader

# Define a dictionary to map MARC tags to field names
FIELD_NAMES = {
    "001": "Control Number",
    "005": "Date and Time of Latest Transaction",
    "100": "Main Entry - Personal Name",
    "101": "Language Code",
    "102": "Country of Publication",
    "135": "Material Designation",
    "200": "Title Statement",
    "210": "Publication, Distribution, etc.",
    "215": "Physical Description",
    "304": "Note - Bibliography",
    "305": "Note - Data",
    "316": "Note - Copy Information",
    "321": "General Note",
    "325": "General Note",
    "620": "Subject Added Entry",
    "700": "Added Entry - Personal Name",
    "790": "Added Entry - Alternative Name",
    "801": "Source of Cataloging",
    "852": "Location",
    "856": "Electronic Location and Access",
    "899": "Local Note"
}

# Path to your MARC file
file_path = 'NLR009671448.mrc'

# Read and parse MARC records
with open(file_path, 'rb') as file:
    reader = MARCReader(file, to_unicode=True, force_utf8=True)
    for record in reader:
        print(f"Processing record: {record['001'].value() if '001' in record else 'Unknown ID'}")
        
        # Iterate through fields
        for field in record.fields:
            if field.tag.isdigit():  # Variable fields
                field_name = FIELD_NAMES.get(field.tag, "Unknown Field")
                print(f"Field: {field.tag} ({field_name}), {field.value()}")
                for subfield_code, subfield_value in field.get_subfields():
                    print(f"  Subfield ${subfield_code}: {subfield_value}")
            else:  # Fixed fields (e.g., LDR)
                print(f"Field: {field.tag} (Leader), Value: {field.value()}")
