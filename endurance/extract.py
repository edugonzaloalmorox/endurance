from utils import RaceLinksScraperBikepacking, RaceLinkScraperDotwatcher, BikePackingScraper, DotWatcherScraper


# Get the links from the races ------- 


# Bikepacking.com
scraper_bikepacking = RaceLinksScraperBikepacking()
base_url_bikepacking = 'https://bikepacking.com/bikes/bikepacking-race-rigs'
num_pages_bikepacking = 5
result_links_bikepacking = scraper_bikepacking.run_scraper(base_url_bikepacking, num_pages_bikepacking)



# Dotwatcher.cc
scraper_dotwatcher = RaceLinkScraperDotwatcher()
base_url_dotwatcher = 'https://dotwatcher.cc/features/bikes-of'
num_pages_dotwatcher = 3
result_links_dotwatcher = scraper_dotwatcher.run_scraper(base_url_dotwatcher, num_pages_dotwatcher)


# Scrape the websites ------------               

## Bikepacking.com
web_scraper_bikepacking = BikePackingScraper(result_links_bikepacking)
bikepacking_df = web_scraper_bikepacking.run_scraper()
bikepacking_df.to_csv('notebooks/data/bikepacking.csv', index=False)



## Dotwatcher.cc


dotwatch_lst = [item for item in result_links_dotwatcher if item not in ['https://dotwatcher.cc/feature/fara-bikes-of-badlands-2023',
                                                           'https://dotwatcher.cc/feature/bikes-of-bc-epic-1000-2023', 
                                                           'https://dotwatcher.cc/feature/bikes-of-dales-divide-2023', 
                                                           'https://dotwatcher.cc/feature/bikes-of-heading-southwest-2022', 
                                                           'https://dotwatcher.cc/feature/bikes-of-ggag-2022', 
                                                           'https://dotwatcher.cc/feature/bikes-of-granguanche-audax-trail'
                                                           ]]

scraper = DotWatcherScraper(dotwatch_lst)
dotwatch_df= scraper.scrape_data()
dotwatch_df.to_csv('notebooks/data/dotwatcher.csv', index=False)

