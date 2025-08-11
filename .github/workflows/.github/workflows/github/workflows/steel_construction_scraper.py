#!/usr/bin/env python3
"""
Simplified Steel Construction Data Scraper for GitHub Actions
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
import json
from datetime import datetime
import logging
from urllib.parse import urljoin
from pathlib import Path
import argparse

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SteelConstructionScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Create output directories
        self.output_dir = Path("steel_construction_data")
        self.output_dir.mkdir(exist_ok=True)
        (self.output_dir / "raw_documents").mkdir(exist_ok=True)
        (self.output_dir / "processed_data").mkdir(exist_ok=True)
        (self.output_dir / "reports").mkdir(exist_ok=True)
        
        # Target categories for steel-intensive construction
        self.target_categories = [
            "structural steel", "precast concrete", 
            "deep foundations", "cast in place concrete"
        ]
        
        # Known URLs from UNC Charlotte (our confirmed source)
        self.test_urls = [
            "https://facilities.charlotte.edu/our-services/business-related-services/capital-projects/business-opportunities/awards-bid-results"
        ]
        
        self.projects_data = []
        self.bids_data = []
    
    def get_page_safely(self, url, retries=3):
        """Fetch webpage with error handling"""
        for attempt in range(retries):
            try:
                time.sleep(2)  # Be respectful
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt == retries - 1:
                    return None
        return None
    
    def find_steel_projects(self, page_url):
        """Find steel-intensive construction projects on a page"""
        logging.info(f"Searching for projects at {page_url}")
        
        response = self.get_page_safely(page_url)
        if not response:
            return []
        
        soup = BeautifulSoup(response.content, 'html.parser')
        projects = []
        
        # Look for PDF links with steel-related keywords
        pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.I))
        
        for link in pdf_links:
            link_text = link.get_text(strip=True)
            pdf_url = urljoin(page_url, link.get('href'))
            
            # Check if this looks like a steel-intensive project
            if self.is_steel_project(link_text):
                project = {
                    'name': link_text,
                    'url': pdf_url,
                    'category': self.identify_category(link_text),
                    'source': page_url,
                    'found_date': datetime.now().isoformat()
                }
                projects.append(project)
                logging.info(f"Found steel project: {link_text}")
        
        return projects
    
    def is_steel_project(self, project_name):
        """Check if project name suggests steel-intensive work"""
        name_lower = project_name.lower()
        
        steel_keywords = [
            'steel', 'precast', 'concrete', 'foundation', 'structural',
            'parking', 'stadium', 'residence', 'academic', 'bid tab'
        ]
        
        return any(keyword in name_lower for keyword in steel_keywords)
    
    def identify_category(self, project_name):
        """Identify which steel category this project belongs to"""
        name_lower = project_name.lower()
        
        if 'steel' in name_lower or 'structural' in name_lower:
            return 'structural_steel'
        elif 'precast' in name_lower:
            return 'precast_concrete'
        elif 'foundation' in name_lower or 'pile' in name_lower:
            return 'deep_foundations'
        elif 'concrete' in name_lower or 'cast' in name_lower:
            return 'cast_in_place_concrete'
        else:
            return 'mixed_steel'
    
    def download_and_analyze_pdf(self, project):
        """Download PDF and extract basic bid information"""
        logging.info(f"Processing: {project['name']}")
        
        # Download PDF
        response = self.get_page_safely(project['url'])
        if not response:
            return None
        
        # Save PDF
        filename = self.clean_filename(project['name']) + '.pdf'
        pdf_path = self.output_dir / "raw_documents" / filename
        
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        
        # Basic analysis (extract text and look for bid amounts)
        bid_data = {
            'project_name': project['name'],
            'category': project['category'],
            'pdf_file': filename,
            'source_url': project['url'],
            'bidders_found': [],
            'bid_amounts': [],
            'processed_date': datetime.now().isoformat()
        }
        
        # Simple text extraction to find dollar amounts
        try:
            import PyPDF2
            with open(pdf_path, 'rb') as file:
                reader = PyPDF2.PdfReader(file)
                text = ""
                for page in reader.pages:
                    text += page.extract_text()
                
                # Look for dollar amounts (basic regex)
                amounts = re.findall(r'\$[\d,]+\.?\d*', text)
                if amounts:
                    bid_data['bid_amounts'] = amounts[:10]  # Keep first 10 amounts found
                
                # Look for contractor names (words ending in Inc, LLC, Corp)
                contractors = re.findall(r'\b[A-Z][a-zA-Z\s]+(?:Inc|LLC|Corp|Company)\b', text)
                if contractors:
                    bid_data['bidders_found'] = list(set(contractors[:5]))  # Keep unique, first 5
        
        except Exception as e:
            logging.warning(f"Could not extract text from {filename}: {e}")
            bid_data['extraction_error'] = str(e)
        
        return bid_data
    
    def clean_filename(self, text):
        """Create safe filename from project name"""
        # Remove special characters and limit length
        clean = re.sub(r'[^\w\s-]', '', text)
        clean = re.sub(r'\s+', '_', clean)
        return clean[:50]  # Limit length
    
    def run_test_collection(self):
        """Run a quick test on known working URLs"""
        logging.info("Starting test collection...")
        
        all_projects = []
        
        # Test on UNC Charlotte (we know this works)
        for url in self.test_urls:
            projects = self.find_steel_projects(url)
            all_projects.extend(projects)
        
        logging.info(f"Found {len(all_projects)} steel projects")
        
        # Process first few projects
        for i, project in enumerate(all_projects[:3]):  # Limit to 3 for testing
            logging.info(f"Processing project {i+1}: {project['name']}")
            bid_data = self.download_and_analyze_pdf(project)
            if bid_data:
                self.bids_data.append(bid_data)
        
        # Save results
        self.save_results()
        return len(self.bids_data)
    
    def run_full_collection(self):
        """Run comprehensive collection (placeholder for now)"""
        logging.info("Running full collection...")
        
        # For now, just run test collection
        # In the future, this would search all UNC campuses
        return self.run_test_collection()
    
    def save_results(self):
        """Save collected data to files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if self.bids_data:
            # Save as CSV
            df = pd.DataFrame(self.bids_data)
            csv_file = self.output_dir / "processed_data" / f"steel_bids_{timestamp}.csv"
            df.to_csv(csv_file, index=False)
            
            # Save as JSON
            json_file = self.output_dir / "processed_data" / f"steel_bids_{timestamp}.json"
            with open(json_file, 'w') as f:
                json.dump(self.bids_data, f, indent=2)
            
            # Create summary
            summary = {
                'collection_date': datetime.now().isoformat(),
                'total_projects': len(self.bids_data),
                'categories': {},
                'files_created': [csv_file.name, json_file.name]
            }
            
            for bid in self.bids_data:
                cat = bid.get('category', 'unknown')
                summary['categories'][cat] = summary['categories'].get(cat, 0) + 1
            
            summary_file = self.output_dir / "reports" / f"summary_{timestamp}.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logging.info(f"Saved {len(self.bids_data)} projects to {csv_file}")
            logging.info(f"Summary: {summary}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--automated', action='store_true', help='Run without user input')
    parser.add_argument('--test', action='store_true', help='Run test collection')
    parser.add_argument('--full', action='store_true', help='Run full collection')
    
    args = parser.parse_args()
    
    scraper = SteelConstructionScraper()
    
    if args.automated:
        print("🏗️ Steel Construction Data Scraper")
        print("=" * 40)
        
        if args.full:
            print("Running full collection...")
            results = scraper.run_full_collection()
        else:
            print("Running test collection...")
            results = scraper.run_test_collection()
        
        print(f"✅ Collection complete! Processed {results} projects")
        print(f"📁 Data saved to: {scraper.output_dir}")
        
    else:
        print("🏗️ Steel Construction Data Scraper")
        print("=" * 40)
        print("1. Test collection (UNC Charlotte only)")
        print("2. Full collection (all sources)")
        
        choice = input("Choose option (1-2): ")
        
        if choice == "1":
            results = scraper.run_test_collection()
        elif choice == "2":
            results = scraper.run_full_collection()
        else:
            print("Invalid choice")
            return
        
        print(f"✅ Complete! Processed {results} projects")

if __name__ == "__main__":
    main()
