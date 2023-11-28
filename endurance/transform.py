
import pandas as pd
import numpy as np
from skimpy import skim
pd.set_option('display.max_columns', None)
from utils import DataProcessor
import os


# Example usage
processor = DataProcessor('bikepacking.csv', 'dotwatcher.csv')
final_data = processor.process_data()
print(final_data.head())
final_data.to_csv('data/processed/endurance_complete.csv', index=False)






