import pandas as pd
import os 
import json
from utils import load_and_collate_results, convert_json_to_df




final_list = load_and_collate_results('data/input/results')

bike_df = convert_json_to_df(final_list)


bike_df.to_csv('data/processed/bike_df.csv', index=False)
print('Bike information saved to data/processed/bike_df.csv !!!!!')
