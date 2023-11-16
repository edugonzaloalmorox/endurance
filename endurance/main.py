from utils import RaceLinksScraperBikepacking, RaceLinkScraperDotwatcher


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

