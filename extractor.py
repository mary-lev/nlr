import pandas as pd
import json
from pathlib import Path
import fitz  # PyMuPDF
import numpy as np
from datetime import datetime
from typing import Dict, List
import hashlib
import PIL.Image
import io

class ImageExtractor:
    def __init__(self, marc_json_path: str, output_dir: Path):
        """
        Initialize extractor with paths and configuration
        """
        self.df = pd.read_json(marc_json_path)
        self.df["year"] = self.df["year"].replace("", 1800)
        self.df["year"] = self.df["year"].replace("5780", 1780)
        self.df.dropna(subset=["year"], inplace=True)
        self.df["year"] = self.df["year"].astype(int)
        self.df = self.df[self.df["pdf_filename"].str.contains("b")]
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.image_metadata = []
        
    def generate_image_id(self, pdf_path: str, page_num: int, image_num: int) -> str:
        """Generate unique ID for each image"""
        unique_string = f"{pdf_path}_{page_num}_{image_num}"
        return hashlib.md5(unique_string.encode()).hexdigest()[:12]
        
    def analyze_image(self, image_bytes: bytes) -> Dict:
        """Analyze image properties using PIL"""
        img = PIL.Image.open(io.BytesIO(image_bytes))
        return {
            'format': img.format,
            'mode': img.mode,
            'width': img.width,
            'height': img.height,
            'size_bytes': len(image_bytes)
        }
        
    def select_pages(self, doc, total_pages: int) -> List[int]:
        """Select pages to extract based on criteria"""
        # Skip first and last 3 pages
        start_page = 5
        end_page = total_pages - 5
        
        # Calculate sampling rate based on book size
        if total_pages < 50:
            step = 5
        elif total_pages < 100:
            step = 8
        else:
            step = 10
            
        return list(range(start_page, end_page, step))
        
    def process_pdf(self, pdf_path: str, marc_record: Dict) -> List[Dict]:
        """Extract images from PDF with metadata"""
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        selected_pages = self.select_pages(doc, total_pages)
        page_metadata = []
        
        for page_num in selected_pages:
            page = doc[page_num]
            image_list = page.get_images()
            
            # Process each image on the page
            for img_idx, image in enumerate(image_list):
                xref = image[0]  # Get the image reference
                base_image = doc.extract_image(xref)
                
                if base_image:  # Check if image extraction was successful
                    # Generate unique ID and filename
                    image_id = self.generate_image_id(pdf_path, page_num, img_idx)
                    image_filename = f"{image_id}.{base_image['ext']}"
                    image_path = self.output_dir / image_filename
                    
                    # Analyze image properties
                    image_analysis = self.analyze_image(base_image["image"])
                    
                    # Save image
                    with open(image_path, "wb") as f:
                        f.write(base_image["image"])
                    
                    # Create metadata entry
                    metadata = {
                        'image_id': image_id,
                        'source': {
                            'pdf_file': pdf_path,
                            'marc_file': marc_record['marc_file'],
                            'control_number': marc_record['control_number'],
                            'year': marc_record['year']
                        },
                        'page_info': {
                            'page_number': page_num,
                            'total_pages': total_pages,
                            'relative_position': page_num / total_pages,
                            'images_on_page': len(image_list),
                            'image_index': img_idx
                        },
                        'image_properties': {
                            'path': str(image_path),
                            'extension': base_image['ext'],
                            'colorspace': base_image['colorspace'],
                            'width': image_analysis['width'],
                            'height': image_analysis['height'],
                            'size_bytes': image_analysis['size_bytes'],
                            'mode': image_analysis['mode'],
                            'original_dpi': base_image.get('xres', 0),
                            'compression': base_image.get('compression', ''),
                        },
                        'extraction_metadata': {
                            'timestamp': datetime.now().isoformat(),
                            'xref': xref,
                            'sampling_rate': f"1/{total_pages//len(selected_pages)}"
                        }
                    }
                    
                    page_metadata.append(metadata)
        
        doc.close()
        return page_metadata 

    def process_all_books(self):
        """Process all books from the DataFrame"""
        for _, row in self.df.iterrows():
            marc_record = {
                'marc_file': row['marc_file'],
                'control_number': row['control_number'],
                'year': row['year']
            }
            
            pdf_path = f"books/{row['pdf_filename']}"
            try:
                page_metadata = self.process_pdf(pdf_path, marc_record)
                self.image_metadata.extend(page_metadata)
            except Exception as e:
                print(f"Error processing {pdf_path}: {str(e)}")
                
        # Save complete metadata
        self.save_metadata()
        
    def save_metadata(self):
        """Save metadata to JSON file"""
        output_path = self.output_dir / 'images_metadata.json'
        
        # Add dataset-level statistics
        metadata_package = {
            'dataset_info': {
                'total_images': len(self.image_metadata),
                'total_books': len(self.df),
                'creation_date': datetime.now().isoformat(),
                'year_distribution': self.get_year_distribution(),
                'average_images_per_book': len(self.image_metadata) / len(self.df)
            },
            'images': self.image_metadata
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(metadata_package, f, indent=2, ensure_ascii=False)
            
    def get_year_distribution(self) -> Dict:
        """Calculate distribution of images across years"""
        years = [img['source']['year'] for img in self.image_metadata]
        return pd.Series(years).value_counts().sort_index().to_dict()

def main():
    # Initialize extractor
    extractor = ImageExtractor(
        marc_json_path="marc_records.json",
        output_dir=Path("./extracted_images")
    )
    
    # Process all books
    extractor.process_all_books()

if __name__ == "__main__":
    main()