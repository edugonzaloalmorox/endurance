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



class BikePackingScraper:
    def __init__(self, flat_list):
        self.flat_list = flat_list
        self.data_frames_list = []

    async def main(self, url):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(url, {'timeout': 60000})

        # Get html
        html = await page.content()
        await browser.close()
        return html

    def run_scraper(self):
        for i in range(len(self.flat_list)):
            print(f'Getting info from:', self.flat_list[i])

            url = self.flat_list[i]
            html_response = asyncio.get_event_loop().run_until_complete(self.main(url))
            soup = BeautifulSoup(html_response, 'html.parser')

            # Names riders ---------------------
            h2_elements = soup.find_all('h2')

            if url == 'https://bikepacking.com/bikes/2023-ascend-armenia-rigs/':
                h3_elements = soup.find_all('h3')
                names_riders = [h3.text for h3 in h3_elements]
                no_names = ['Inspiration', 'Gear/Reviews', 'The Gear Index', 'Bikepacking Bikes', 'Plan Your Trip', 'Essential Reading', 'Where to Begin', 'By Location', 'By Type', 'By Length (days)']
                names_riders = [name for name in names_riders if name not in no_names]
            else:
                names_riders = [h2.text for h2 in h2_elements]

            dict_names = {'names': names_riders, 'url': url}

            # Demographics ---------------------
            h4_element = soup.find_all('h4', style=lambda value: value and value.startswith('margin-top:'))

            demographics = []
            for i in range(len(h4_element)):
                demographics.append(h4_element[i].text)

            age = []
            location = []

            for item in demographics:
                age_match = re.search(r'Age (\d+)', item)
                if age_match:
                    age.append(int(age_match.group(1)))
                else:
                    age.append('No info')

                location_match = re.search(r'/ (.+)', item)
                if location_match:
                    location.append(location_match.group(1))
                else:
                    location.append('No info')

            country_list = [re.search(r'\((.*?)\)', item).group(1) if re.search(r'\((.*?)\)', item) else 'No info' for item in location]

            # Get info from kit list ---------------------
            p_element = soup.find_all('p')

            headers = ['ROUTE:', 'BIKE:', 'BAGS:', 'GEAR HIGHLIGHTS:']

            route_info = []
            bike_info = []
            bags_info = []
            gear_info = []

            kit_list = []

            for i, p_element in enumerate(p_element):
                paragraph_text = p_element.get_text()

                if any(header in paragraph_text for header in headers):
                    kit_list.append(paragraph_text)

            info_dict = {header: [] for header in headers}

            for i in range(len(kit_list)):
                for header in headers:
                    pattern = re.compile(fr'{re.escape(header)}(.*?)(?=\n|$)', re.DOTALL)
                    match = pattern.search(kit_list[i])

                    if match:
                        info = match.group(1).strip()
                        info_dict[header].append(info)
                    else:
                        info_dict[header].append('No info')

            route_info = info_dict['ROUTE:']
            bike_info = info_dict['BIKE:']
            bags_info = info_dict['BAGS:']
            gear_info = info_dict['GEAR HIGHLIGHTS:']

            # Title ---------------------
            h1_element = soup.find('h1')
            title = h1_element.text.strip('Rigs of ')

            race_info = {'Name': names_riders,
                         'Age': age,
                         'Country': country_list,
                         'Location': location,
                         'Bike': bike_info,
                         'Bags': bags_info,
                         'Gear': gear_info,
                         'Route': route_info,
                         }

            list_lengths = {key: len(value) for key, value in race_info.items()}
            max_length = max(list_lengths.values())

            for key, length in list_lengths.items():
                if race_info[key] is not None:
                    race_info[key] = race_info[key] + ['No info'] * (max_length - length) if length < max_length else race_info[key][:max_length]

            race_info = {
                'Name': race_info['Name'],
                'Age': race_info['Age'],
                'Country': race_info['Country'],
                'Location': race_info['Location'],
                'Bike': race_info['Bike'],
                'Bags': race_info['Bags'],
                'Gear': race_info['Gear'],
                'Route': race_info['Route'],
                'Race': title,
                'Link': url
            }

            df_iteration = pd.DataFrame(race_info)
            self.data_frames_list.append(df_iteration)

        # Concatenate all DataFrames in the list into a single DataFrame
        df_endurance = pd.concat(self.data_frames_list, ignore_index=True)
        return df_endurance
    
class DotWatcherScraper:
    def __init__(self, links_list):
        self.links_list = links_list
        self.data_frames_list = []

    async def main(self, url):
        browser = await launch()
        page = await browser.newPage()
        await page.goto(url, {'timeout': 60000})

        # Get html
        html = await page.content()
        await browser.close()
        return html

    def scrape_data(self):
        for i in range(len(self.links_list)):
            print(f'Getting info from:', self.links_list[i])
            url = self.links_list[i]
            url_dict = {'Link': url}

            # Get soup object
            html_response = asyncio.get_event_loop().run_until_complete(self.main(url))
            soup = BeautifulSoup(html_response, 'html.parser')

            # Get the race
            race = soup.find('h1').text
            race_dict = {'Race': race}

            different_structure_links = ['https://dotwatcher.cc/feature/bikes-of-basajaun-2023',
                                         'https://dotwatcher.cc/feature/bikes-of-log-drivers-waltz-2023',
                                         'https://dotwatcher.cc/feature/bikes-of-the-ardennes-monster-2023']

            if any(link in url for link in different_structure_links):
                div_elements = soup.find_all('div', class_='sc-30963e4a-4 fXZkBS')
            else:
                div_elements = soup.find_all('div', class_='sc-30963e4a-5 gYOzwm')

            bike_list = []
            bike_dict = {}

            for div_element in div_elements:
                paragraphs = div_element.find_all('p')
                for paragraph in paragraphs:
                    strong_element = paragraph.find('strong')
                    if strong_element:
                        bike_dict[strong_element.text] = paragraph.text
                        bike_dict.update(race_dict)
                        bike_dict.update(url_dict)

                bike_list.append(bike_dict.copy())

            df_output = pd.DataFrame(bike_list)

            columns_to_remove_string = ['Name', 'Race', 'Bike', 'Age', 'Location', 'Key items of kit', 'Cap number']

            strings_to_remove = {'Name': 'Name:',
                                'Race': 'Bikes of ',
                                'Bike': 'Bike:',
                                'Location': 'Location:',
                                'Age': 'Age:',
                                'Key items of kit': 'Key items of kit:',
                                'Cap number': 'Cap number:'}

            for column in columns_to_remove_string:
                try:
                    df_output[column] = self.remove_string(df_output[column], strings_to_remove[column])
                except KeyError:
                    df_output[column] = np.nan

            df_output['Country'] = df_output['Location'].str.split(',').str[1].str.strip()

            df_iteration = df_output.dropna(how='all')
            self.data_frames_list.append(df_iteration)

        return pd.concat(self.data_frames_list, ignore_index=True)

    @staticmethod
    def remove_string(column, string_to_remove):
        return column.str.lstrip(string_to_remove).str.strip()