import pandas as pd
import numpy as np
from skimpy import skim
pd.set_option('display.max_columns', None)

from dotenv import load_dotenv
import os
import json
import concurrent.futures
from openai import OpenAI, OpenAIError, RateLimitError
import backoff


load_dotenv()
OpenAI.api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI()
print('Client created')


print('Loading data...')
df = pd.read_csv('notebooks/data/endurance_complete.csv')
df_2023 = df[df['year']==2023]



print('Loading prompts...')
prompt = "You are a helpful assistant of a bike shop that speaks only in JSON. Do not generate output that isn’t in properly formatted JSON and don't add extra information. Your task it to extract information for: Bike brand, Bike model, Wheels, Gears, Speed, Tyres, Size of the Tyres and Lights. \n\nExample text \n\"BMC Kaius 01 ONE with integrated aero bar/stem and bottle cages. Profile Design clip-on aerobar extensions, SRAM Red wireless shifting, 38T chainring with 10-44T cassette (no doubt under-geared here…), 165mm cranks for my short legs, Zipp 303 Firecrest wheels, Pirelli Cinturato 700x40c tyres, WTB SL8 saddle, Supernova M99 Mini Pro B54 front light.\"\n\nBike brand: BMC \nBike model: Kaius 01 ONE\nWheels: Zipp 303 Firecrest \nGears: SRAM Red \nSpeeds: 38T chainring with 10-44T\nTyres: Pirelli Cinturato \nSize Tyre:  700x40c\nLights: Supernova M99 Mini Pro B54\n\n\"Ridley Kanzo Adventure 2023. Tubeless Vittoria Mezcal 29 x2.1″ tires.\"\n\nBike brand: Ridley \nBike model: Kanzo Adventure 2023\nWheels: NA \nGears: NA\nSpeeds: NA\nTyres:  Vittoria Mezcal\nSize Tyre:  29 x2.1\nLights: NA\n\nIn case there is no information available in any variable fill with NA\n"


print('Creating completions...')

import os
import json
import concurrent.futures
from openai import OpenAI, OpenAIError, RateLimitError
import backoff


@backoff.on_exception(backoff.expo, RateLimitError)
def completions_with_backoff(**kwargs):
    response = client.chat.completions.create(**kwargs)
    return response

def process_single_bike(bike_text):
    user_message = {
        "role": "user",
        "content": bike_text
    }

    messages = [
        {
            "role": "system",
            "content": prompt
        },
        user_message
    ]

    response = completions_with_backoff(
        model="gpt-3.5-turbo-1106",
        messages=messages,
        temperature=0,
        max_tokens=256,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )

    output_content = response.choices[0].message.content
    
    # Raise an exception if 'No info' is found
    if output_content.lower() == 'no info':
        raise ValueError(f"No information available for bike: {bike_text}")

    return output_content
    
    
    

def process_bike_info_test_parallel(bike_list, race_to_filter=None):
    # Create a directory to save the results
    save_directory = 'results'
    os.makedirs(save_directory, exist_ok=True)

    responses_list = []

    with concurrent.futures.ThreadPoolExecutor() as executor:
        try:
            # Process the bike list in parallel
            results = list(executor.map(process_single_bike, bike_list))

            # Save results in separate files with 500 results each
            chunk_size = 10
            for i in range(0, len(results), chunk_size):
                chunk = results[i:i + chunk_size]
                file_num = i // chunk_size + 1
                file_path = os.path.join(save_directory, f'results_{file_num}.json')
                with open(file_path, 'w') as f:
                    json.dump(chunk, f)

            responses_list.extend(results)
        except ValueError as e:
            print(f"Caught an exception: {e}")
    
    cleaned_data = [item.replace('\n', '') for item in responses_list]
    cleaned_data = [item.replace('```', '') for item in cleaned_data]
    cleaned_data = [item.replace('json', '') for item in cleaned_data]

    return cleaned_data


def run_in_batches(bike_list, batch_size=25):
    num_batches = len(bike_list) // batch_size
    remainder = len(bike_list) % batch_size

    all_results = []

    for i in range(num_batches):
        start_index = i * batch_size
        end_index = start_index + batch_size
        bikes_chunk = bike_list[start_index:end_index]
        print(f"Processing batch {i + 1}/{num_batches}")
        batch_results = process_bike_info_test_parallel(bikes_chunk)
        all_results.extend(batch_results)

    if remainder > 0:
        last_batch = bike_list[-remainder:]
        print(f"Processing the remaining {remainder} bikes")
        last_batch_results = process_bike_info_test_parallel(last_batch)
        all_results.extend(last_batch_results)

    return all_results



def process_and_save_results(list_to_subset, batch_size, start_index, end_index):
    
    bikes_subset = list_to_subset[start_index:end_index]
    
    output_results = run_in_batches(bikes_subset, batch_size)
    
    output_file_path = f'results/output_results_{start_index}{end_index}.json'
    
    with open(output_file_path, 'w') as f:
        json.dump(output_results, f) 
    
    print(f'Results for bikes {start_index} to {end_index} saved to {output_file_path}')



print('Processing completions...')


bike_list = df_2023['bike'].tolist()
batch_size = 25

# Set the desired range of indices
start_index = 751
end_index = 800

process_and_save_results(bike_list, batch_size, start_index, end_index)



'''
bike_list = df_2023['bike'].tolist()
bikes_test = bike_list[601:650]
output_results = run_in_batches(bikes_test, batch_size=25)

print('Print results...')
print(output_results)


# save output as output_results as json file

with open(f'results/output_results_{601_650}.json', 'w') as f:
    json.dump(output_results, f)

'''