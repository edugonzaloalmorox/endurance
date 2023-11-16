import asyncio
from pyppeteer import launch
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import nest_asyncio
import re
import numpy as np


class RaceLinksScraperBikepacking:
    def __init__(self):
        self.links_races = []

    async def fetch_html(self, url):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(url, {'timeout': 60000})
        html = await page.content()
        await browser.close()
        return html

    def scrape_bikepacking_links(self, url):
        html_response = asyncio.get_event_loop().run_until_complete(self.fetch_html(url))
        soup = BeautifulSoup(html_response, 'html.parser')
        ul_element = soup.find('ul', class_='postlist')

        link_list = []
        if ul_element:
            for li_element in ul_element.find_all('li'):
                a_tag = li_element.find('a')
                if a_tag and 'href' in a_tag.attrs:
                    link_list.append(a_tag['href'])

        self.links_races.append(link_list)

    def run_scraper(self, base_url, num_pages):
        for i in range(1, num_pages + 1):
            url = f'{base_url}/page/{i}/'
            self.scrape_bikepacking_links(url)
            print(f'Finished page: {i}!!')

        flat_list = [item for sublist in self.links_races for item in sublist]
        return flat_list


class RaceLinkScraperDotwatcher:
    def __init__(self):
        self.links_races = []

    async def fetch_html(self, url):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(url, {'timeout': 60000})
        html = await page.content()
        await browser.close()
        return html

    def scrape_dotwatcher_links(self, base_url, num_pages):
        for i in range(1, num_pages + 1):
            url = f'{base_url}?page={i}'
            html_response = asyncio.get_event_loop().run_until_complete(self.fetch_html(url))
            soup = BeautifulSoup(html_response, 'html.parser')
            div_element = soup.find_all('div', class_='sc-c9625ead-0 kbWUBN')

            refs_lst = []

            for div in div_element:
                a_tag = div.find('a')
                if a_tag and 'href' in a_tag.attrs:
                    refs_lst.append(a_tag['href'])

            # Add the base url to the list of links
            base_url_dotwatcher = 'https://dotwatcher.cc'
            links = [base_url_dotwatcher + link for link in refs_lst]
            self.links_races.append(links)

            print(f'Finished page: {i}!!')

    def run_scraper(self, base_url, num_pages):
        self.scrape_dotwatcher_links(base_url, num_pages)
        flat_list = [item for sublist in self.links_races for item in sublist]
        return flat_list
