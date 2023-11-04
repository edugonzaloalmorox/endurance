import asyncio
from pyppeteer import launch
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd

# write code to create a soup object from html file

async def main(url):
    browser = await launch()
    page = await browser.newPage()
    await page.goto(url)
    
    # Get html
    
    html = await page.content()
    await browser.close()
    return html






html_response = asyncio.get_event_loop().run_until_complete(main('https://bikepacking.com/bikes/bikepacking-race-rigs/page/2/'))
        


    ## Load html response into BeautifulSoup

soup = BeautifulSoup(html_response, 'html.parser')
ul_element = soup.find('ul', class_= 'postlist')

link_list = []
    # Extract href from each <li>
if ul_element:
    for li_element in ul_element.find_all('li'):
            # Find the <a> tag within the <li>
            a_tag = li_element.find('a')
            
            # Extract the href attribute
            if a_tag and 'href' in a_tag.attrs:
                link_list.append(a_tag['href'])
  
print(link_list)


