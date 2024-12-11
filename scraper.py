from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
import asyncio
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    filename=f'scraper_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def process_single_item(page, button, index):
    try:
        logging.info(f"Processing item {index}")
        
        # Wait for the new tab with timeout
        async with page.expect_popup() as popup_info:
            await button.click()
        new_page = await popup_info.value
        
        try:
            # Click on "Карточка" tab
            await new_page.click('text="Карточка"')
            await new_page.wait_for_timeout(2000)
            
            # Get bibliographic description
            bibl_link = new_page.locator('text=Полное библиографическое описание')
            
            async with new_page.expect_popup() as rusmarc_popup_info:
                await bibl_link.click()
            rusmarc_page = await rusmarc_popup_info.value
            await rusmarc_page.wait_for_load_state('networkidle')
            
            # Download RUSMARC
            try:
                rusmarc_link = rusmarc_page.locator('text=RUSMARC ISO2709')
                if await rusmarc_link.count() > 0:
                    async with rusmarc_page.expect_download() as download_info:
                        await rusmarc_link.click()
                    download = await download_info.value
                    await download.save_as(f"{download.suggested_filename}")
                    logging.info(f"Successfully downloaded RUSMARC file for item {index}")
            except Exception as e:
                logging.error(f"Error downloading RUSMARC for item {index}: {str(e)}")
            
            await rusmarc_page.close()
            
            # Main file download process
            try:
                download_button = new_page.locator('a#btn-download[onclick="registerDownload()"]')
                await download_button.click()
                
                await new_page.wait_for_selector('text="Файл №1"', timeout=60000)
                popup_download = await new_page.wait_for_selector('.btn-download-part.button:has-text("Скачать")')
                
                async with new_page.expect_download() as download_info:
                    await popup_download.click()
                
                download = await download_info.value
                await download.save_as(f"{download.suggested_filename}")
                logging.info(f"Successfully downloaded main file for item {index}")
                
                await new_page.wait_for_selector('text="Закрыть"')
                await new_page.click('text="Закрыть"')
            except Exception as e:
                logging.error(f"Error downloading main file for item {index}: {str(e)}")
            
        finally:
            await new_page.close()
            
    except PlaywrightTimeoutError:
        logging.error(f"Timeout error processing item {index}")
    except Exception as e:
        logging.error(f"Unexpected error processing item {index}: {str(e)}")

async def main():
    retry_count = 3
    retry_delay = 5  # seconds
    
    for attempt in range(retry_count):
        try:
            async with async_playwright() as p:
                logging.info("Starting browser launch...")
                browser = await p.chromium.launch(headless=False)
                logging.info("Browser launched successfully")
                
                logging.info("Creating browser context...")
                context = await browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                logging.info("Browser context created")
                
                logging.info("Creating new page...")
                page = await context.new_page()
                logging.info("New page created")
                
                try:
                    logging.info("Navigating to URL...")
                    await page.goto('https://primo.nlr.ru/primo-explore/search?query=lsr31,contains,Русская%20книга%20гражданской%20печати%20XVIII%20в.,AND&tab=default_tab&search_scope=A1XVIII_07NLR&vid=07NLR_VU2&mfacet=tlevel,include,online_resources,1&mode=advanced&offset=20&came_from=pagination_3_4', 
                                  timeout=60000)  # Adding explicit timeout
                    logging.info("Successfully navigated to URL")
                    
                    await page.wait_for_timeout(1000)
                    
                    buttons = await page.locator('button.neutralized-button:has-text("Электронная копия")').all()
                    logging.info(f"Found {len(buttons)} items to process")
                    
                    for index, button in enumerate(buttons, 1):
                        await process_single_item(page, button, index)
                        await page.wait_for_timeout(1000)  # Delay between items
                        
                    logging.info("Successfully completed processing all items")
                    return  # Success - exit the retry loop
                    
                except Exception as e:
                    logging.error(f"Error during page processing: {str(e)}")
                    raise
                
                finally:
                    await browser.close()
                    
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < retry_count - 1:
                logging.info(f"Waiting {retry_delay} seconds before retrying...")
                await asyncio.sleep(retry_delay)
            else:
                logging.error("All retry attempts failed")
                raise

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except Exception as e:
        logging.error(f"Script terminated with error: {str(e)}")