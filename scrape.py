from playwright.async_api import async_playwright
import asyncio
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
import json
import os

async def process_page(page, url, records):
    await page.goto(url)
    await page.wait_for_timeout(2000)
    
    buttons = await page.locator('button.neutralized-button:has-text("Электронная копия")').all()
    print(f"Found {len(buttons)} buttons on this page")
    
    for i, button in enumerate(buttons):
        try:
            print(f"Processing button {i+1} of {len(buttons)}")
            async with page.expect_popup() as popup_info:
                await button.click()
            new_page = await popup_info.value
            
            await new_page.click('text="Карточка"')
            await new_page.wait_for_timeout(2000)
            bibl_link = new_page.locator('text=Полное библиографическое описание')
            rusmarc_url = await bibl_link.get_attribute('href')
                       
            # Download main file
            download_button = new_page.locator('a#btn-download[onclick="registerDownload()"]')
            await download_button.click()
            
            await new_page.wait_for_selector('text="Файл №1"', timeout=60000)
            popup_download = await new_page.wait_for_selector('.btn-download-part.button:has-text("Скачать")')
            
            async with new_page.expect_download() as download_info:
                await popup_download.click()
            
            download = await download_info.value
            filename = download.suggested_filename
            await download.save_as(filename)
            
            # Save record to dictionary
            records.append({
                "rusmarc_url": rusmarc_url,
                "downloaded_file": filename,
            })
            
            # Save JSON after each successful download
            with open('download_records.json', 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            
            await new_page.wait_for_selector('text="Закрыть"')
            await new_page.click('text="Закрыть"')
            
            await new_page.close()
            await page.wait_for_timeout(1000)
            
        except Exception as e:
            print(f"Error processing button {i+1}: {str(e)}")
            continue

async def main():
    base_url = 'https://primo.nlr.ru/primo-explore/search?query=lsr31,contains,%D0%A0%D1%83%D1%81%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BA%D0%BD%D0%B8%D0%B3%D0%B0%20%D0%B3%D1%80%D0%B0%D0%B6%D0%B4%D0%B0%D0%BD%D1%81%D0%BA%D0%BE%D0%B9%20%D0%BF%D0%B5%D1%87%D0%B0%D1%82%D0%B8%20XVIII%20%D0%B2.,AND&tab=default_tab&search_scope=A1XVIII_07NLR&vid=07NLR_VU2&mfacet=tlevel,include,online_resources,1&mode=advanced'
    
    # Define offset range
    start_offset = 80
    end_offset = 100
    step = 10  # Number of items per page
    
    # Load existing records if file exists
    records = []
    if os.path.exists('download_records.json'):
        with open('download_records.json', 'r', encoding='utf-8') as f:
            records = json.load(f)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        
        # Process pages within the offset range
        for offset in range(start_offset, end_offset + 1, step):
            try:
                # Construct URL with current offset
                parsed_url = urlparse(base_url)
                params = parse_qs(parsed_url.query)
                params['offset'] = [str(offset)]
                new_query = urlencode(params, doseq=True)
                parts = list(parsed_url)
                parts[4] = new_query
                current_url = urlunparse(parts)
                
                print(f"Processing offset {offset}")
                await process_page(page, current_url, records)
                print(f"Completed processing offset {offset}")
                
            except Exception as e:
                print(f"Error processing offset {offset}: {str(e)}")
                continue
        
        await browser.close()

if __name__ == '__main__':
    asyncio.run(main())