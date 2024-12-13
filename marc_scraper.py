import json
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from typing import List, Dict

# Set up logging
logging.basicConfig(
    filename=f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class MARCDownloader:
    def __init__(self, records: List[Dict], output_dir: str = "marc"):
        self.records = records
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.browser = None
        self.context = None
        self.page = None
        
    async def setup(self):
        """Initialize browser and context"""
        try:
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=False,
                args=['--disable-dev-shm-usage']  # Helps with memory issues
            )
            self.context = await self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            self.page = await self.context.new_page()
            
            # Set default timeouts
            self.page.set_default_timeout(60000)
            self.page.set_default_navigation_timeout(60000)
            
        except Exception as e:
            logging.error(f"Setup failed: {str(e)}")
            await self.cleanup()
            raise

    async def cleanup(self):
        """Clean up resources"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
        except Exception as e:
            logging.error(f"Cleanup error: {str(e)}")

    async def download_marc_file(self, record: Dict) -> bool:
        """Download a single MARC file"""
        try:
            link = record['rusmarc_url']
            logging.info(f"Processing URL: {link}")
            
            # Navigate to page
            await self.page.goto(link)
            await self.page.wait_for_load_state('networkidle')
            
            # Look for download link
            rusmarc_link = self.page.locator('text=RUSMARC ISO2709')
            if await rusmarc_link.count() == 0:
                logging.warning(f"No RUSMARC download link found for {link}")
                return False
            
            # Download file
            async with self.page.expect_download(timeout=30000) as download_info:
                await rusmarc_link.click()
                
            download = await download_info.value
            output_path = self.output_dir / download.suggested_filename
            
            await download.save_as(output_path)
            logging.info(f"Successfully downloaded: {output_path}")
            
            # Verify file exists and has content
            if not output_path.exists() or output_path.stat().st_size == 0:
                logging.error(f"Download verification failed for {output_path}")
                return False
                
            return True
            
        except PlaywrightTimeoutError:
            logging.error(f"Timeout while processing {link}")
            return False
        except Exception as e:
            logging.error(f"Error downloading MARC file: {str(e)}")
            return False

    async def process_all_records(self):
        """Process all records with retry logic"""
        retry_count = 3
        retry_delay = 5
        
        try:
            await self.setup()
            
            for record in self.records:
                success = False
                
                for attempt in range(retry_count):
                    if attempt > 0:
                        logging.info(f"Retry attempt {attempt + 1} for {record['rusmarc_url']}")
                        await asyncio.sleep(retry_delay * attempt)
                    
                    success = await self.download_marc_file(record)
                    if success:
                        break
                
                if not success:
                    logging.error(f"Failed to download after {retry_count} attempts: {record['rusmarc_url']}")
                
        except Exception as e:
            logging.error(f"Process failed: {str(e)}")
            raise
        finally:
            await self.cleanup()

async def main():
    try:
        # Load records
        with open('download_records.json', 'r', encoding='utf-8') as f:
            records = json.load(f)
        
        downloader = MARCDownloader(records)
        await downloader.process_all_records()
        
    except Exception as e:
        logging.error(f"Main process failed: {str(e)}")
        raise

if __name__ == '__main__':
    asyncio.run(main())