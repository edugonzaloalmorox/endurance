import pandas as pd
import os 
import json
from utils import load_and_collate_results, convert_json_to_df




final_list = load_and_collate_results('data/test_inputs')



different_bikes = convert_json_to_df(final_list)
print(different_bikes.tail())
print(f'Size of the bikes_df is:', len(different_bikes))

different_bikes.to_csv('data/processed/different_bikes.csv', index=False)
print('Bike information saved to data/processed/different_bikes.csv !!!!!')

'''
# Create data for 2023 
bike_df = pd.read_csv('data/processed/endurance_complete.csv')
df_2023 = bike_df[bike_df['year'] == 2023]
#print(df_2023.head())
'''