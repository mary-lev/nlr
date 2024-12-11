from playwright.async_api import async_playwright
import asyncio

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        page = await browser.new_page()
        await page.goto('https://primo.nlr.ru/primo-explore/search?query=lsr31,contains,%D0%A0%D1%83%D1%81%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BA%D0%BD%D0%B8%D0%B3%D0%B0%20%D0%B3%D1%80%D0%B0%D0%B6%D0%B4%D0%B0%D0%BD%D1%81%D0%BA%D0%BE%D0%B9%20%D0%BF%D0%B5%D1%87%D0%B0%D1%82%D0%B8%20XVIII%20%D0%B2.,AND&tab=default_tab&search_scope=A1XVIII_07NLR&vid=07NLR_VU2&mfacet=tlevel,include,online_resources,1&mode=advanced&offset=30&came_from=pagination_4_5')
        await page.wait_for_timeout(1000)
        buttons = await page.locator('button.neutralized-button:has-text("Электронная копия")').all()
        print(buttons)
        #buttons = await page.locator('button.neutralized-button.arrow-link-button:has(span.availability-status)').all()
    
        for button in buttons[4:]:
            print(button)
            # Wait for the new tab to open when clicking the button
            async with page.expect_popup() as popup_info:
                await button.click()
            new_page = await popup_info.value
        
            # Click on "Карточка" tab
            await new_page.click('text="Карточка"')
            await new_page.wait_for_timeout(2000)
            bibl_link = new_page.locator('text=Полное библиографическое описание')
        
            # This click will open another page
            async with new_page.expect_popup() as rusmarc_popup_info:
                await bibl_link.click()
            rusmarc_page = await rusmarc_popup_info.value
            await rusmarc_page.wait_for_load_state('networkidle', timeout=120000)
        
            # Now find and click RUSMARC link on the new page
            rusmarc_link = rusmarc_page.locator('text=RUSMARC ISO2709')
            if await rusmarc_link.count() > 0:
                async with rusmarc_page.expect_download() as download_info:
                    await rusmarc_link.click()
                download = await download_info.value
                await download.save_as(f"{download.suggested_filename}")
        
            # Close RUSMARC page
            await rusmarc_page.close()
        
            download_button = new_page.locator('a#btn-download[onclick="registerDownload()"]')
            await download_button.click()
        
            # Wait for the popup dialog and its download button
            await new_page.wait_for_selector('text="Файл №1"', timeout=60000) 
            # Wait for the download button in the popup window
            popup_download = await new_page.wait_for_selector('.btn-download-part.button:has-text("Скачать")')
        
            # Handle the download
            async with new_page.expect_download() as download_info:
                await popup_download.click()
            
            download = await download_info.value
            # Save the file with its suggested filename
            await download.save_as(f"{download.suggested_filename}")
        
            # Wait for popup to close or close it manually if needed
            await new_page.wait_for_selector('text="Закрыть"')
            await new_page.click('text="Закрыть"')
        
            # Close the new tab
            await new_page.close()
        
            # Optional: delay between processing items
            await page.wait_for_timeout(1000)
        
       
    await browser.close()

if __name__ == '__main__':
    asyncio.run(main())