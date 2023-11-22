
import pandas as pd
import numpy as np
from skimpy import skim
pd.set_option('display.max_columns', None)
from utils import DataProcessor
import os


# Example usage
processor = DataProcessor('bikepacking.csv', 'dotwatcher.csv')
final_data = processor.process_data()
final_data.to_csv('notebooks/data/endurance_complete.csv', index=False)




