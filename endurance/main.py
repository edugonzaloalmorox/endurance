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




links_races = []
for i in range(1,6):
    print(f'Getting page: https://bikepacking.com/bikes/bikepacking-race-rigs/page/{i}/')
    
    url = f'https://bikepacking.com/bikes/bikepacking-race-rigs/page/{i}/'
    html_response = asyncio.get_event_loop().run_until_complete(main(url))
            


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
    
    links_races.append(link_list)
    print(f'Finished page: {i}!!')


# Flatten link_races list  

flat_list = [item for sublist in links_races for item in sublist]

print(flat_list)


