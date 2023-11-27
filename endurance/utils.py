import asyncio
from pyppeteer import launch
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import nest_asyncio
import re
import numpy as np
import os


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
        print('Starting bikepakcing links scraper...')
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
        print('Starting dotwatcher links scraper...')
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
    
    
    

class DataProcessor:
    def __init__(self, bikepacking_file, dotwatch_file):
        print("Load Bikepacking file.... ")
        self.bikepacking = pd.read_csv(f'notebooks/data/{bikepacking_file}')
        print("Load Dotwatch file.... '")
        self.dotwatch = pd.read_csv(f'notebooks/data/{dotwatch_file}')

    def tidy_bikepacking_data(self):
        print("Tidy Bikepacking file.... ")
        self.bikepacking.columns = self.bikepacking.columns.str.lower()
        self.bikepacking['age'] = pd.to_numeric(self.bikepacking['age'], errors='coerce')
        self.bikepacking['race'] = self.bikepacking['race'].str.replace('the', '', case=False)
        self.bikepacking['race'] = self.bikepacking['race'].str.replace('The', '', case=False)
        self.bikepacking['source'] = 'bikepacking.com'
        self.bikepacking =  self.bikepacking[['name', 'age', 'country', 'location', 'bike', 'bags', 'gear', 'race', 'link', 'source']]
        return self.bikepacking
        

    def tidy_dotwatch_data(self):
        print("Tidy Dotwatch file.... ")
        self.dotwatch.columns = self.dotwatch.columns.str.lower()
        self.dotwatch = self.dotwatch[['name', 'age', 'country', 'location', 'bike', 'key items of kit', 'race', 'link']]
        self.dotwatch = self.dotwatch.iloc[:, 0:6].merge(self.dotwatch.iloc[:, 7:10], left_index=True, right_index=True)
        return self.dotwatch

    def clean_countries(self):
        print("Clean countries Dotwatch file.... ")
        problem_country = self.dotwatch[self.dotwatch['country'].isnull()]

        # Define country mapping
        country_mapping = {
            'Oxfordshire/Uni of Bath': 'United Kingdom',
            'Glasgow': 'United Kingdom',
            'Italy': 'Italy',
            'Norway': 'Norway',
            'Istraland': 'Italy',  
            'Bassano del Grappa': 'Italy',
            'Bochum': 'Germany',
            'Germany': 'Germany',
            'Portugal/Luxembourg': 'Portugal',
            'NZ': 'New Zealand',
            'Slovenia': 'Slovenia',
            'Austria': 'Austria',
            'Croatia': 'Croatia',
            'London': 'United Kingdom',
            'France': 'France',
            'Girona': 'Spain',
            'South East London': 'United Kingdom',
            'Masies de Voltregà (SPAIN)': 'Spain',
            'Paris': 'France',
            'Truro': 'United Kingdom',
            'Canterbury': 'United Kingdom',
            'New Forest': 'United Kingdom',
            'Bexley': 'United Kingdom',
            'St Ives Cornwall': 'United Kingdom',
            'Warwickshire': 'United Kingdom',
            'Surrey': 'United Kingdom',
            'Falkensee (Brandenburg/GER)': 'Germany',
            'Mülheim an der Ruhr Deutschland': 'Germany',
            'Leichlingen (Germnany)': 'Germany',
            'Berlin': 'Germany',
            'Baden bei Wien': 'Austria',
            'Halstenbek': 'Germany',
            'Amsterdam': 'Netherlands',
            'Düsseldorf': 'Germany',
            'Lithuania': 'Lithuania',
            'New Zealand': 'New Zealand',
            'Ingolstadt': 'Germany',
            'Sittard (NL)': 'Netherlands',
            'Innsbruck': 'Austria',
            'Lucca': 'Italy',
            'Dresden Germany': 'Germany',
            'Rosenheim': 'Germany',
            'Jena': 'Germany',
            'Llangynidr': 'United Kingdom',
            'Durham UK': 'United Kingdom',
            'Colombia': 'Colombia',
            'Frankfurt': 'Germany',
            'Vienna': 'Austria',
            'The Netherlands': 'Netherlands',
            'Australia': 'Australia',
            'Rotterdam': 'Netherlands',
            'BIELSKO BIAŁA': 'Poland',
            'Belgium': 'Belgium',
            'Netherlands': 'Netherlands',
            'Utrecht': 'Netherlands',
            'Diemen': 'Netherlands',
            'Woerden': 'Netherlands',
            'Zeeland': 'Netherlands',
            'Köln': 'Germany',
            'Newport on Tay': 'United Kingdom',
            'Koblenz (Germany)': 'Germany',
            'Cornwall': 'United Kingdom',
            'Isle of Wight': 'United Kingdom',
            'Bury St Edmunds': 'United Kingdom',
            'South Wales': 'United Kingdom',
            'Wiltshire': 'United Kingdom',
            'Montréal': 'Canada',
            'Ottawa': 'Canada',
            'Pakenham': 'Australia',
            'Saint Sauveur': 'Canada',
            'Toronto': 'Canada',
            'Saint-Sauveur': 'Canada',
            'MONTRÉAL': 'Canada',
            'Portgruaro': 'Italy',
            'Cadiar': 'Spain',
            'The Netherlands / Amerongen': 'Netherlands',
            'Salamanca': 'Spain',
            'Windsor': 'United Kingdom',
            'Bergamo (ITA)': 'Italy',
            'Madrid': 'Spain',
            'Turkiye': 'Turkey',
            'Munich (Germany)': 'Germany',
            'Switzerland': 'Switzerland',
            "l'Alcúdia (Spain)": 'Spain',
            'Castelldefels': 'Spain',
            'Sevilla': 'Spain',
            'Toledo': 'Spain',
            'Girona (Catalunya)': 'Spain',
            'Spain': 'Spain',
            'Copenhagen': 'Denmark',
            'London (UK)': 'United Kingdom',
            'Thunder Bay Ontario Canada': 'Canada',
            'Barcelona': 'Spain',
            'Sabero (León': 'Spain',
            'Bilbao': 'Spain',
            'Valladolid': 'Spain',
            'Zaragoza': 'Spain',
            'GBR': 'United Kingdom',
            'Riaydh': 'Saudi Arabia',
            'Traunstein': 'Germany',
            'Genval Belgium': 'Belgium',
            'Tongeren': 'Belgium',
            'Venice': 'Italy',
            'Dorset': 'United Kingdom',
            'Brighton': 'United Kingdom',
            'Darkest Dorset': 'United Kingdom',
            'Kingston upon Thames': 'United Kingdom',
            'Bristol': 'United Kingdom',
            'Glossop': 'United Kingdom',
            'South Australia': 'Australia',
            'Carpi': 'Italy',
            'weil am rhein': 'Germany',
            'Holzkirchen': 'Germany',
            'London // UK': 'United Kingdom',
            'München': 'Germany',
            'Aarau': 'Switzerland',
            'LONDON': 'United Kingdom',
            'Munich': 'Germany',
            'Frankfurt (Germany)': 'Germany',
            'Poland': 'Poland',
            'Vienna Austria': 'Austria',
            'Plymouth': 'United Kingdom',
            'Rixensart - Belgium': 'Belgium',
            'Oswestry Shropshire': 'United Kingdom',
            'Darlington': 'United Kingdom',
            'Pembrokeshire': 'United Kingdom',
            'Abergele': 'United Kingdom',
            'Scarborough': 'United Kingdom',
            'East Yorkshire': 'United Kingdom',
            'North Derby': 'United Kingdom',
            'Eastbourne': 'United Kingdom',
            'Lincolnshire': 'United Kingdom',
            'Royal Leamington Spa': 'United Kingdom',
            'Oxford': 'United Kingdom',
            'Redhill': 'United Kingdom',
            'Kiruna': 'Sweden',
            'Stockholm': 'Sweden',
            'Alingsås': 'Sweden',
            'Brussels': 'Belgium',
            'California': 'United States',
            'Bavaria': 'Germany',
            'Helsinki Finland': 'Finland',
            'an Australian in London': 'Australia',
            'Leipzig/Germany': 'Germany',
            'Bend Oregon USA': 'United States',
            'Suisse': 'Switzerland',
            'Bergen': 'Norway',
            'Lausanne': 'Switzerland',
            'Leipzig': 'Germany',
            'Slovakia 🇬🇧': 'Slovakia',
            'Portugal': 'Portugal',
            'Utiel (Valencia)': 'Spain',
            'Toulouse': 'France',
            'UK': 'United Kingdom',
            'Sweden': 'Sweden',
            'MADRID': 'Spain',
            'Ariège': 'France',
            'Lyon': 'France',
            'El Prat de Llobregat': 'Spain',
            'Sopelana': 'Spain',
            'Banbury': 'United Kingdom',
            'Northants': 'United Kingdom',
            'Lincoln': 'United Kingdom',
            'Birmingham': 'United Kingdom',
            'Essex': 'United Kingdom',
            'nan': 'Unknown',
            'England': 'United Kingdom',
            'Manchester': 'United Kingdom',
            "'t Harde NL": 'Netherlands',
            'Harrogate UK': 'United Kingdom',
            'Aalsmeer The Netherlands': 'Netherlands',
            'Purmerend Netherlands': 'Netherlands',
            'Driebergen-Rijsenburg': 'Netherlands',
            'Nederland': 'Netherlands',
            'Deutschland': 'Germany',
            'Heusden-Zolder (Belgium)': 'Belgium',
            'Wehrheim / Germany': 'Germany',
            'Schöneck / Germany': 'Germany',
            'Merelbeke': 'Belgium',
            'Hereford': 'United Kingdom',
            'Shropshire': 'United Kingdom',
            'Cardiff': 'United Kingdom',
            'Wales': 'United Kingdom',
            'Romsey': 'United Kingdom',
            'Malvern': 'United Kingdom',
            'Slovenija': 'Slovenia',
            'Italy.Roma': 'Italy',
            'Farndon UK': 'United Kingdom',
            'Derventa BiH': 'Bosnia and Herzegovina',
            'Las Palmas de Gran Canaria': 'Spain',
            'Versailles France': 'France',
            'Banja Luka': 'Bosnia and Herzegovina',
            'Athens Greece': 'Greece',
            'Gernany': 'Germany',
            'Vienna/Austria': 'Austria',
            'Köln / Germany': 'Germany',
            'Frankfurt am Main': 'Germany',
            'Trieste': 'Italy',
            'Auerbach/Germany': 'Germany',
            'Luebeck': 'Germany',
            'Eindhoven': 'Netherlands',
            'Czech Republic': 'Czech Republic',
            'Chelmsford': 'United Kingdom',
            'Down south': 'Unknown',
            'West sussex': 'United Kingdom',
            'Twickenham': 'United Kingdom',
            'Colchester,': 'United Kingdom',
            'High Wycombe': 'United Kingdom',
            'London UK': 'United Kingdom',
            'Whitstable': 'United Kingdom',
            'Rutland': 'United Kingdom',
            'Cheshire': 'United Kingdom',
            'Colchester': 'United Kingdom',
            'Southampton': 'United Kingdom',
            'Norfolk': 'United Kingdom',
            'Abingdon': 'United Kingdom',
            'Burgess Hill': 'United Kingdom',
            'Stoke Newington - London': 'United Kingdom',
            'Wageningen': 'Netherlands',
            'West Sussex': 'United Kingdom',
            'Ashford Kent': 'United Kingdom',
            'Roquetes': 'Spain',
            'Florence': 'Italy',
            'La Seu d’Urgell': 'Spain',
            'Manresa': 'Spain',
            'Amposta': 'Spain',
            'Palencia': 'Spain',
            'Edinburgh': 'United Kingdom',
            'Royal Wootton Bassett': 'United Kingdom',
            'Antwerp': 'Belgium',
            'Sheffield': 'United Kingdom',
            'Devon UK': 'United Kingdom',
            'Braunschweig (Germany)': 'Germany',
            'Cumbria': 'United Kingdom',
            'Harrogate': 'United Kingdom',
            'Nomad': 'Unknown',
            'Scottish Highlands': 'United Kingdom',
            'Hassloch': 'Germany',
            'Mittelgebirge Classique 2023!': 'Germany',
            'Hemmingen': 'Germany',
            'Dresden': 'Germany',
            'Le Mans / France': 'France',
            'Wrocław': 'Poland',
            'Poznań / Poland': 'Poland',
            'Chepstow': 'United Kingdom',
            'eydon': 'Unknown',
            'Folkestone': 'United Kingdom',
            'Exeter': 'United Kingdom',
            'Frome': 'United Kingdom',
            'Palmela': 'Portugal',
            'Benedita': 'Portugal',
            'Murrhardt - Germany': 'Germany',
            'Bergamo': 'Italy',
            'Ireland': 'Ireland',
            'Kent': 'United Kingdom',
            'Kidderminster': 'United Kingdom',
            'Nottingham': 'United Kingdom',
            'Bath': 'United Kingdom',
            'Caerleon': 'United Kingdom',
            'Somerset UK': 'United Kingdom',
            'Celje - Slovenia': 'Slovenia',
            'Siegen': 'Germany',
            'Leipzig / Germany': 'Germany',
            'leipzig / Germany': 'Germany',
            'San Pietro di Feletto': 'Italy',
            'Forli': 'Italy',
            'Cesiomaggiore (BL) Italy': 'Italy',
            'SEGUSINO TREVISO ITALY': 'Italy',
            'Solagna': 'Italy',
            'Nantes (FR)': 'France',
            'Konstanz': 'Germany',
            'Bilbao (SPAIN': 'Spain',
            'Garmisch Partykirchen': 'Germany',
            'Bern': 'Switzerland',
            'Zürich': 'Switzerland',
            'Basel': 'Switzerland',
            'Hachenburg': 'Germany',
            'Lokeren / Belgium': 'Belgium',
            'Marseille bébé': 'France',
            'Lille': 'France',
            'Antwerp - Belgium': 'Belgium',
            'CALDERS': 'Unknown',
            'Cracow (Poland)': 'Poland',
            'Germany (Munich)': 'Germany',
            'Letterkenny': 'Ireland',
            'Marseille FRANCE': 'France',
            'Northern Ireland': 'United Kingdom',
            'Wexford Ireland': 'Ireland',
            'Coín Malaga España': 'Spain',
            'Hamburg': 'Germany',
            'Heidelberg Germany': 'Germany',
            'Peak District': 'United Kingdom',
            'Montreal': 'Canada',
            'Göttingen Germany': 'Germany',
            'Orzola': 'Spain',
            'Lausanne (Switzerland)': 'Switzerland',
            'Dubai': 'United Arab Emirates',
            'Munich (Germany) and Girona (Spain)': 'Germany',
            'UAE': 'United Arab Emirates',
            'Canadian living in Namibia': 'Namibia',
            'Rwanda': 'Rwanda',
            'Chile' : 'Chile',
            'Mexico': 'Mexico',
            'A Canadian based in Dubai.': 'United Arab Emirates', 
            "Scl": "Country Not Found",
            "Brasil": "Brazil",
            "Viña Del Mar. Chile": "Chile",
            "Santiago de Chile": "Chile",
            "Bariloche Argentina": "Argentina",
            "Guatemala": "Guatemala",
            "CDMX/Argentina": "Argentina",
            "Santiago": "Chile",
            "Santiago. Chile": "Chile",
            "Temuco": "Chile",
            "Brazil": "Brazil",
            "Liverpool": "United Kingdom",
            "Capbreton France": "France",
            "Worldwide": "Country Not Found",
            "Vaugneray France": "France",
            "68": "Country Not Found",
            "In a hut (Refuge du Ruhle)": "Country Not Found",
            "Dutch living in France Alps (Puy Saint Vincent)": "France",
            "Leeds": "United Kingdom",
            "People's Republic of Kernow (Cornwall)": "United Kingdom",
            "LDN": "United Kingdom",
            "Forest of Dean": "United Kingdom",
            "Indonesia": "Indonesia",
            "Bandung": "Indonesia",
            "Bogor": "Indonesia",
            "Jakarta": "Indonesia",
            "Jakarat": "Indonesia",
            "Vilanova i la Geltrú": "Spain",
            "Coventry": "United Kingdom",
            "Chapel En Le Frith": "United Kingdom",
            "Lewes": "United Kingdom",
            "Denmark": "Denmark",
            "German/ based in Switzerland": "Switzerland",
            "Dartmouth. Nova Scotia. Canada": "Canada",
            "Leipzig/GER": "Germany",
            "in train": "Country Not Found",
            "Tewkesbury": "United Kingdom",
            "Leighton Buzzard": "United Kingdom",
            "Carlisle": "United Kingdom",
            "Swindon": "United Kingdom",
            "Wiltshire UK": "United Kingdom",
            "Perthshire": "United Kingdom",
            "FOCC BRISTOL": "United Kingdom",
            "Usk": "United Kingdom",
            "Warrington": "United Kingdom",
            "Seva": "Country Not Found",
            "Singapore": "Singapore",
            "España": "Spain",
            "Ins": "Country Not Found",
            "Bonn": "Germany",
            "Zürich (Switzerland)": "Switzerland",
            "Steinhuserberg / Switzerland": "Switzerland",
            "Torino": "Italy",
            "Bologna Italy": "Italy",
            "Enskede": "Country Not Found",
            "Taiwan": "Taiwan",
            "Inverness": "United Kingdom",
            "Disley": "United Kingdom",
            "Durham": "United Kingdom",
            "Aviemore": "United Kingdom",
            "German/ based in Basel (CH)": "Switzerland",
            "Amiens from france": "France",
            "Getxo": "Spain",
            "Maspujols": "Spain",
            "Manlleu": "Spain",
            "Reus (Catalunya)": "Spain",
            "Torelló": "Spain",
            "Zaragoza (Spain)": "Spain",
            "Lugo": "Spain",
            "Dresden; Germany": "Germany",
            "Warsaw": "Poland",
            "Dresden - Germany": "Germany",
            "Kraków": "Poland",
            "Krakow": "Poland",
            "Gdańsk": "Poland",
            "Budapest": "Hungary",
            "Conegliano Veneto Italy": "Italy",
            "Salzburg": "Austria",
            "Mallorca Spain": "Spain",
            "Würzburg Germany": "Germany",
            "Pécs": "Hungary",
            "Rouen (France)": "France",
            "Esslingen (Germany)": "Germany",
            "Warnsveld": "Netherlands",
            "Sint-Michielsgestel": "Netherlands",
            "Cambridge": "United Kingdom",
            "Vreeswijk": "Country Not Found",
            "Den Haag": "Netherlands",
            "Prague": "Czech Republic",
            "Gleisdorf/Austria": "Austria",
            "Helmond": "Netherlands",
            "Germany Oldenburg": "Germany",
            "Amersfoort": "Netherlands",
            "Sneek": "Netherlands",
            "CzechRepublic": "Czech Republic",
            "Scotland": "United Kingdom",
            "Koblenz Germany": "Germany",
            "Lithuanian living in Norway": "Norway",
            "Nelson": "New Zealand",
            "Christchurch": "New Zealand",
            "Rotorua NZ": "New Zealand",
            "Timaru": "New Zealand",
            "Kapiti Coast": "New Zealand",
            "Auckland": "New Zealand",
            "Taupo": "New Zealand",
            "Wellington": "New Zealand",
            "Drury": "New Zealand",
            "Rotorua": "New Zealand",
            "Hamilton": "New Zealand",
            "Kingston": "Country Not Found",
            "Wanaka": "New Zealand",
            }

        # Create a DataFrame from the mapping
        df_country = pd.DataFrame.from_dict(country_mapping, orient='index')
        df_country.reset_index(inplace=True)
        df_country.columns = ['location', 'country']

        # Merge the mapping with the problematic data
        problem_country = problem_country.drop(columns=['country'])
        problem_country = problem_country.merge(df_country, on='location', how='left')
        problem_country = problem_country[['name', 'age', 'country', 'location', 'bike', 'key items of kit', 'race', 'link']]

        # Concatenate the cleaned data with the original data
        print("Add clean countries to dotwatch.... ")
        dotwatch_not_null = self.dotwatch[self.dotwatch['country'].notnull()]
        self.dotwatch_clean = pd.concat([dotwatch_not_null, problem_country], axis=0)

        self.dotwatch_clean['source'] = 'dotwatcher.cc'
        self.dotwatch_clean['bags'] = np.nan
        self.dotwatch_clean['gear'] = self.dotwatch_clean['key items of kit']
        self.dotwatch_clean = self.dotwatch_clean[self.bikepacking.columns]

        return self.dotwatch_clean
        
    
    def concatenate_data(self):
        print("Concatenate dataframes.... ") 
        self.df_final = pd.concat([self.bikepacking, self.dotwatch_clean], axis=0)
        return self.df_final
    
    def extract_year(self, df):
        self.df_final['year'] = self.df_final['link'].str.extract(r'(2018|2019|2020|2021|2022|2023)').astype(float)
        return self.df_final

    def clean_location_country(self):
        # Define the location-country mapping
            location_country_dict = {
        'USA': 'United States',
        'Switzerland': 'Switzerland',
        'Canada': 'Canada',
        'Ecuador': 'Ecuador',
        'No info': 'No information',
        'Spain': 'Spain',
        'The Netherlands': 'Netherlands',
        'Germany': 'Germany',
        'Italy': 'Italy',
        'Finland': 'Finland',
        'Ireland': 'Ireland',
        'Belgium': 'Belgium',
        'Portugal': 'Portugal',
        'Philippines': 'Philippines',
        'Scotland': 'Scotland',
        'United Kingdom': 'United Kingdom',
        'Denmark': 'Denmark',
        'South Africa': 'South Africa',
        'France': 'France',
        'Poland': 'Poland',
        'Austria': 'Austria',
        'Sweden': 'Sweden',
        'Norway': 'Norway',
        'Australia': 'Australia',
        'England': 'England',
        'Kyrgyzstan': 'Kyrgyzstan',
        'Kazakhstan': 'Kazakhstan',
        'Netherlands': 'Netherlands',
        'Czech Republic': 'Czech Republic',
        'Hungary': 'Hungary',
        'China': 'China',
        'New Zealand': 'New Zealand',
        'Swtizerland': 'Switzerland',  # Corrected typo
        'Russia': 'Russia',
        'Indonesia': 'Indonesia',
        'Latvia': 'Latvia',
        'UK': 'United Kingdom',
        'Georgia': 'Georgia',
        'Armenia': 'Armenia',
        'Belarus': 'Belarus',
        'Estonia': 'Estonia',
        'Wales': 'Wales',
        'FRANCE': 'France',
        'Romania': 'Romania',
        'South Korea': 'South Korea',
        'Greece': 'Greece',
        'on Gadigal land': 'Australia',
        'Morocco': 'Morocco',
        'Israel': 'Israel',
        'Madagascar': 'Madagascar',
        'North Ireland': 'Northern Ireland',
        'Canary Islands': 'Canary Islands',
        'South of France': 'France',
        'Oslo': 'Norway',
        'Basque Country': 'Spain',
        'United kingdom': 'United Kingdom',
        'Granada': 'Spain',
        'Caracas/Venezuela': 'Venezuela',
        'Northern Ireland': 'Northern Ireland',
        'Venezuela': 'Venezuela',
        'Turkey': 'Turkey',
        'Slovakia': 'Slovakia',
        'NZ': 'New Zealand',
        'Taiwan': 'Taiwan',
        'AUS': 'Australia',
        'Atlanta, Georgia, USA': 'United States',
        'Born and raised in Papua New Guinea': 'Papua New Guinea',
        'Catalonia': 'Spain',
        'St. Pete Beach': 'United States',
        'Lithuania': 'Lithuania',
        'Colorado': 'United States',
        'Lativa': 'Latvia',
        'Amah Mutsun': 'Unknown',  # No specific country mentioned
        'ITALY': 'Italy',
        'GER': 'Germany',
        '390ft': 'Unknown',  # No specific country mentioned
        'Aboriginal community': 'Unknown',  # No specific country mentioned
        'CA': 'Canada',
        'St Pete Beach': 'United States',
        'CANADA': 'Canada',
        'currently residing in Alexandria, VA': 'United States',
        'but proudly Dutch': 'Netherlands',
        'CZE': 'Czech Republic',
        'Yorkshire Dales': 'United Kingdom',
        'uk': 'United Kingdom',
        'Cornwall': 'United Kingdom',
        'Czech republic': 'Czech Republic',
        'Croatia': 'Croatia',
        'Slovenia': 'Slovenia',
        'Pyrénées Atlantiques': 'France',
        'Antwerp': 'Belgium',
        'Bergisches Land': 'Germany',
        'Somerset': 'United Kingdom',
        'Essex': 'United Kingdom',
        'Deutschland': 'Germany',
        'South Wales': 'United Kingdom',
        'Canarias': 'Spain',
        'NL': 'Netherlands',
        'Nederland': 'Netherlands',
        'Netherland': 'Netherlands',
        'Bad Hersfeld': 'Germany',
        'the Netherlands': 'Netherlands',
        'Miraflores de la Sierra': 'Spain',
        'Arkansas': 'United States',
        'North York Moors': 'United Kingdom',
        'Gywnedd': 'United Kingdom',
        'Ontario': 'Canada',
        'NY': 'United States',
        'Québec': 'Canada',
        'VT': 'United States',
        'Qc': 'Canada',
        'ON': 'Canada',
        'QC': 'Canada',
        'Quebec': 'Canada',
        'Euskal Herria': 'Spain',
        'Basque country': 'Spain',
        'España': 'Spain',
        'Barcelona': 'Spain',
        'UK.': 'United Kingdom',
        'Colombia': 'Colombia',
        'Chile': 'Chile',
        'Scotland.': 'Scotland',
        'West Sussex. UK': 'United Kingdom',
        'Dorset': 'United Kingdom',
        'France.': 'France',
        'Rutland': 'United Kingdom',
        'North Wales': 'United Kingdom',
        'Gelderland': 'Netherlands',
        'Wales.': 'United Kingdom',
        'Sussex': 'United Kingdom',
        'Channel Islands': 'United Kingdom',
        'East Sussex': 'United Kingdom',
        'Czechia': 'Czech Republic',
        'WA (USA)': 'United States',
        'Kaltland': 'Unknown',  # No specific country mentioned
        'Norway.': 'Norway',
        'Utah': 'United States',
        'Hannover': 'Germany',
        'Hamburg': 'Germany',
        'Oregon USA': 'United States',
        'spain': 'Spain',
        'Hampshire': 'United Kingdom',
        'Berks': 'United Kingdom',
        'GR': 'Greece',
        'Cumbria': 'United Kingdom',
        'Denmark. From the UK.': 'United Kingdom',
        'Cologne': 'Germany',
        'RLP': 'Germany',
        'Kerry': 'Ireland',
        'Liberties': 'Ireland',
        'NI': 'Northern Ireland',
        'South Shropshire': 'United Kingdom',
        'Shropshire': 'United Kingdom',
        'Cheshire': 'United Kingdom',
        'UK (originally from Novocherkassk': 'United Kingdom',
        'Bosnia and Herzegovina': 'Bosnia and Herzegovina',
        'Bulgaria': 'Bulgaria',
        'South Tyrol': 'Italy',
        'United Arab Emirates': 'United Arab Emirates',
        'Como': 'Italy',
        'originally from Arnstorf (Germany)': 'Germany',
        'California': 'United States',
        'Northants': 'United Kingdom',
        'Derbyshire': 'United Kingdom',
        'Isle of Wight': 'United Kingdom',
        'West Sussex': 'United Kingdom',
        'Devon': 'United Kingdom',
        'North Yorkshire': 'United Kingdom',
        'Kent': 'United Kingdom',
        'kent': 'United Kingdom',
        'Lancashire': 'United Kingdom',
        'Penrith': 'United Kingdom',
        'UT': 'United States',
        'Highlands': 'United Kingdom',
        'Co. Meath': 'Ireland',
        'Greater Manchester': 'United Kingdom',
        'Fife': 'United Kingdom',
        'Germnay': 'Germany',  # Corrected typo
        'Schweiz': 'Switzerland',
        'PL': 'Poland',
        'Polska': 'Poland',
        'Aveiro Portugal': 'Portugal',
        'Portugal.': 'Portugal',
        'Washington': 'United States',
        'German7': 'Germany',
        'Tirol': 'Austria',
        'Surrey': 'United Kingdom',
        'Catalunya': 'Spain',
        'Lincolnshire': 'United Kingdom',
        'Suisse': 'Switzerland',
        'Ticino': 'Switzerland',
        'france': 'France',  # Corrected lowercase entry
        'Savoie': 'France',
        'Auatria': 'Austria',  # Corrected typo
        'North Wales.': 'United Kingdom',
        'Ukraine': 'Ukraine',
        'Región de Los Lagos': 'Chile',
        'Brazil': 'Brazil',
        'Brasil': 'Brazil',
        'Mexico': 'Mexico',
        'V Región de Valparaíso': 'Chile',
        'RS': 'Brazil',
        'Buenos Aires': 'Argentina',
        'Paraná/Brasil': 'Brazil',
        'Chile (Spanish nationality)': 'Chile',
        'Región de la Araucanía': 'Chile',
        'chile': 'Chile',
        'CHILE': 'Chile',
        'Argentina': 'Argentina',
        'Utrecht': 'Netherlands',
        'Luxembourg': 'Luxembourg',
        'Pyrénées Orientales': 'France',
        'FR': 'France',
        'Charente': 'France',
        'Le Nooooorrrd de la France': 'France',
        'Swizterland': 'Switzerland',  # Corrected typo
        'Northumberland': 'United Kingdom',
        'Swansea': 'United Kingdom',
        'Yorkshire': 'United Kingdom',
        'Malaysia': 'Malaysia',
        'Tangerang Selatan': 'Indonesia',
        'Uk': 'United Kingdom',
        'Niedersachsen': 'Germany',
        'Lower Saxony': 'Germany',
        'Argyll and Bute': 'United Kingdom',
        'QC Canada': 'Canada',
        'Aberdeenshire': 'United Kingdom',
        'England.': 'United Kingdom',
        'MO': 'United States',
        'Cornwall & La Ciotat': 'United Kingdom',  # Combined two locations
        'Berkshire': 'United Kingdom',
        'Belgium.': 'Belgium',
        "Principat d'Andorra": 'Andorra',
        'Spain.': 'Spain',
        'Basque Country.': 'Spain',
        'Colombia.': 'Colombia',
        'Sonnenbühl': 'Germany',
        'Perugia': 'Italy',
        'Tenerife': 'Spain',
        'Catalunya.': 'Spain',
        'Republic of Moldova': 'Moldova',
        'AT': 'Austria',
        'Austria.': 'Austria',
        'Nijkerk.': 'Netherlands',
        'Noord Brabant': 'Netherlands',
        'Kenya': 'Kenya',
        'France / Rwanda': 'France/Rwanda',  # Combined two locations
        'Rwanda': 'Rwanda',
        'Golden Bay': 'New Zealand',
        'Wellington': 'New Zealand',
        'Saudi Arabia': 'Saudi Arabia',
        'United States': 'United States',
        'nan': 'Unknown',  # Placeholder for missing data
        'Unknown': 'Unknown',
        'Namibia': 'Namibia',
        'Country Not Found': 'Unknown',
        'Guatemala': 'Guatemala',
        'Singapore': 'Singapore',
    }

        # Use the mapping to clean the 'country' column
            self.df_final['country'] = self.df_final['country'].map(location_country_dict).fillna(self.df_final['country'])

            return self.df_final
                

    def process_data(self):
        
        self.tidy_bikepacking_data()
        self.tidy_dotwatch_data()
        self.clean_countries()
        self.concatenate_data()
        self.extract_year(self.df_final)
        self.clean_location_country()

        # Additional processing if needed

        return self.df_final

# Collate the bike output ----- 

def load_and_collate_results(folder_path):
    all_json_data = []

    # Get a sorted list of files in the folder
    files_to_process = sorted(
        (filename for filename in os.listdir(folder_path) if filename.startswith('output_results_') and filename.endswith('.json')),
        key=lambda x: int(x.split('_')[2].split('.')[0])  # Extract and sort by the numeric part of the file name
    )

    # Iterate over the sorted files
    for filename in files_to_process:
        file_path = os.path.join(folder_path, filename)
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            all_json_data.append(json_data)

    collated_data = [json_string for sublist in all_json_data for json_string in sublist]
    return collated_data


def convert_json_to_df(list_data):


    bike_brands = []
    bike_models = []
    wheels = []
    gears = []
    speeds = []
    tyres = []
    tyre_sizes = []
    lights = []

    # Iterate through each JSON entry and extract values for each column
    for json_entry in list_data:
        json_data = eval(json_entry)  # Convert the string to a dictionary
        bike_brands.append(json_data.get("Bike brand", ""))
        bike_models.append(json_data.get("Bike model", ""))
        wheels.append(json_data.get("Wheels", ""))
        gears.append(json_data.get("Gears", ""))
        speeds.append(json_data.get("Speeds", ""))
        tyres.append(json_data.get("Tyres", ""))
        tyre_sizes.append(json_data.get("Size Tyre", ""))
        lights.append(json_data.get("Lights", ""))

    df = pd.DataFrame({
        'bike_brand': bike_brands,
        'bike_model': bike_models,
        'wheels': wheels,
        'gears': gears,
        'speeds': speeds,
        'tyres': tyres,
        'tyre_size': tyre_sizes,
        'lights': lights
    })

    return df   