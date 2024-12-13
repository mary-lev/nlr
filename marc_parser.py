import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict, field
from pymarc import MARCReader
from collections import Counter

@dataclass
class MARCField:
    tag: str
    name: str
    value: str
    subfields: Optional[List[Dict[str, str]]] = None

@dataclass
class MARCRecord:
    control_number: str
    fields: List[MARCField]
    marc_file: Optional[str] = None
    urls: Optional[List[str]] = field(default_factory=list)
    pdf_filename: Optional[str] = None
    year: Optional[str] = None

class MARCParser:
    # Define field names mapping
    FIELD_NAMES = {
        "001": "Control Number",
        "005": "Date and Time of Latest Transaction",
        "100": "Main Entry - Personal Name",
        "101": "Language Code",
        "102": "Country of Publication",
        "135": "Material Designation",
        "140": "Coded Data Field",
        "200": "Title Statement",
        "210": "Publication, Distribution, etc.",
        "215": "Physical Description",
        "305": "Note - Data",
        "310": "Note - Binding Information",
        "314": "Note - Responsibility",
        "316": "Note - Copy Information",
        "317": "Provenance Note",
        "321": "General Note",
        "325": "General Note",
        "399": "Local Note",
        "454": "Translation Of",
        "517": "Other Variant Titles",
        "518": "Title in Standard Modern Spelling",
        "620": "Subject Added Entry",
        "700": "Added Entry - Personal Name",
        "702": "Added Entry - Personal Name",
        "712": "Added Entry - Corporate Name",
        "801": "Source of Cataloging",
        "852": "Location",
        "856": "Electronic Location and Access",
        "899": "Local Note",

        # Adding previously unknown fields found in documentation
        "105": "Field of Coded Data: Textual Resources, Monographic",
        "205": "Edition Statement",
        "300": "General Notes",
        "303": "Note - Data", # Not found in documentation
        "304": "Note - Bibliography",
        "306": "Note - Data", # Not found in documentation  
        "307": "Note - Data", # Not found in documentation
        "327": "Notes About Contents",
        "451": "Other Edition on Similar Medium",
        "461": "Set Level",
        "481": "Also Bound in This Volume",
        "482": "Bound With",
        "600": "Personal Name Used as Subject",
        "606": "Topical Name Used as Subject",
        "686": "Other Classification Numbers",
        "701": "Personal Name - Alternative Responsibility",
        "790": "Personal Name - Alternative Form",

        "035": "Other System Numbers",
        "320": "Bibliography / Index Note",
        "330": "Summary or Abstract",
        "412": "Source of Excerpt or Offprint",
        "422": "Parent of Supplement",
        "464": "Analytical Level",
        "488": "Other Related Works",
        "510": "Parallel Title",
        "513": "Added Title-Page Title",
        "514": "Caption Title",
        "601": "Corporate Body Name Used as Subject",
        "602": "Family Name Used as Subject",
        "607": "Geographical Name Used as Subject",
        "610": "Uncontrolled Subject Terms",
        "710": "Corporate Body Name - Primary Responsibility",
        "711": "Corporate Body Name - Alternative Responsibility",
        "722": "Family Name - Secondary Responsibility",
        "791": "Corporate Body Name - Alternative Form",
        "830": "General Note",
    }

    def __init__(self):
        self.unknown_fields = Counter()
        self.unknown_field_examples = {}

    def log_unknown_field(self, tag: str, value: str):
        """Log unknown field and store an example of its content"""
        self.unknown_fields[tag] += 1
        if tag not in self.unknown_field_examples:
            self.unknown_field_examples[tag] = value[:200]  # Store first 200 chars as example

    def extract_pdf_filename(self, urls: List[str]) -> Optional[str]:
        """Extract PDF filename from view URL."""
        for url in urls:
            if "/view" in url:
                # Updated pattern to handle URLs with parameters after 'view'
                match = re.search(r'/([a-z]{2}\d+)/view(?:#.*)?$', url)
                if match:
                    return f"{match.group(1)}.pdf"
            
                # If first pattern doesn't match, try alternative pattern
                match = re.search(r'/([a-z]{2}\d+)/', url)
                if match:
                    return f"{match.group(1)}.pdf"
        print(f"Could not extract PDF filename from URLs: {urls}")
        return None

    def extract_year(self, value: str) -> Optional[str]:
        """Extract year from a string."""
        match = re.search(r'\b(\d{4})\b', value)
        return match.group(1) if match else None

    def parse_file(self, file_path: str, debug: bool = False) -> List[MARCRecord]:
        """Parse a single MARC file and return a list of records."""
        records = []
        marc_file = Path(file_path)
        
        with open(file_path, 'rb') as file:
            reader = MARCReader(file, to_unicode=True, force_utf8=True)
            
            for record in reader:
               
                control_number = record['001'].value() if '001' in record else 'Unknown ID'
                
                fields = []
                urls = []  # Store URLs from 856 fields
                year = ""
                
                # Process all fields in the record
                for field in record:
                   
                    # Skip empty fields
                    if not field:
                        continue
                    
                    # Check if field is unknown
                    if field.tag not in self.FIELD_NAMES:
                        self.log_unknown_field(field.tag, field.value())
                    
                    field_name = self.FIELD_NAMES.get(field.tag, "Unknown Field")
                    
                    # Handle control fields (001-009)
                    if field.tag < '010':
                        fields.append(MARCField(
                            tag=field.tag,
                            name=field_name,
                            value=field.data
                        ))
                        continue
                    
                    # Handle data fields (010 and above)
                    try:
                        field_value = field.value()
                        
                        # Store URLs from 856 fields
                        if field.tag == '856':
                            urls.append(field_value)
                        if field.tag == '210':
                            year = self.extract_year(field_value)
                        
                        subfields = None
                        if hasattr(field, 'subfields') and field.subfields:
                            subfields = [
                                {"code": code, "value": value}
                                for code, value in zip(field.subfields[::2], field.subfields[1::2])
                            ]
                        
                        fields.append(MARCField(
                            tag=field.tag,
                            name=field_name,
                            value=field_value,
                            subfields=subfields
                        ))
                        
                   
                    except Exception as e:
                        if debug:
                            print(f"Error processing field {field.tag}: {str(e)}")
                        continue
                # Extract PDF filename from URLs
                pdf_filename = self.extract_pdf_filename(urls)

                records.append(MARCRecord(
                    control_number=control_number,
                    fields=fields,
                    marc_file=marc_file.name,
                    urls=urls,
                    pdf_filename=pdf_filename,
                    year=year
                ))
        
        return records

    def process_directory(self, directory_path: str, output_path: str, debug: bool = False):
        """Process all MARC files in a directory and save as JSON."""
        dir_path = Path(directory_path)
        all_records = []
        
        for marc_file in dir_path.glob("*.mrc"):
            
            try:
                records = self.parse_file(str(marc_file), debug=debug)
                all_records.extend(records)
                
           
            except Exception as e:
                print(f"Error processing {marc_file}: {str(e)}")
        
        # Convert to JSON-serializable format
        json_records = [
            {
                "control_number": record.control_number,
                "fields": [asdict(field) for field in record.fields],
                "marc_file": record.marc_file,
                "urls": record.urls,
                "pdf_filename": record.pdf_filename,
                "year": record.year
            }
            for record in all_records
        ]
        
        # Save to JSON file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(json_records, f, ensure_ascii=False, indent=2)
        
        return len(all_records)

    def save_unknown_fields_report(self, output_path: str):
        """Save a report of unknown fields encountered during parsing."""
        report = {
            "summary": {
                "total_unique_unknown_fields": len(self.unknown_fields),
                "total_unknown_field_occurrences": sum(self.unknown_fields.values())
            },
            "unknown_fields": [
                {
                    "tag": tag,
                    "occurrences": count,
                    "example": self.unknown_field_examples[tag]
                }
                for tag, count in sorted(self.unknown_fields.items())
            ]
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

def main():
    # Example usage
    parser = MARCParser()
    input_directory = "marc"  # Directory containing .mrc files
    output_file = "marc_records.json"
    unknown_fields_report = "unknown_fields_report.json"
    
    # Process the records
    record_count = parser.process_directory(input_directory, output_file, debug=True)
    print(f"\nProcessed {record_count} records. Output saved to {output_file}")
    
    # Save unknown fields report
    parser.save_unknown_fields_report(unknown_fields_report)
    print(f"\nUnknown fields report saved to {unknown_fields_report}")
    
    # Print summary of unknown fields
    print("\nUnknown fields summary:")
    for tag, count in sorted(parser.unknown_fields.items()):
        print(f"Field {tag}: {count} occurrences")

if __name__ == "__main__":
    main()