import pandas as pd
import os 
import json
from utils import load_and_collate_results, convert_json_to_df




final_list = load_and_collate_results('results')

bike_df = convert_json_to_df(final_list)
