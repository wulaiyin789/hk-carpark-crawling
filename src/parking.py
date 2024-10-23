import requests
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

basic_info_url = 'https://resource.data.one.gov.hk/td/carpark/basic_info_all.json'
carpark_info_url = 'https://api.data.gov.hk/v1/carpark-info-vacancy?data=info'
carpark_vacancy_url = 'https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy'
only_vacancy_url = 'https://resource.data.one.gov.hk/td/carpark/vacancy_all.json'

lang_versions = ['en_US', 'zh_TW', 'zh_CN']

output_dir = 'dist'
file_name = 'carpark_data.json'

# Ensure the output directory exists
if not os.path.exists(output_dir):
  os.makedirs(output_dir)

# Merge two vacancy results into one
def merge_both_vacancy_api(carpark_vacancy_data, result_dict, only_vacancy_data):
  # First URL transformation
  for park in carpark_vacancy_data['results']:
    park_id = park['park_Id']
    vehicle_types = []

    # Dynamically handle all vehicle types present in park
    for vehicle_type, vacancy_info_list in park.items():
      if vehicle_type != 'park_Id':
        for vacancy_info in vacancy_info_list:
          vehicle_types.append({
              'type': vehicle_type,
              'service_category': [{
                  'category': 'HOURLY',  # Assuming "HOURLY" as the category
                  'vacancy_type': vacancy_info.get('vacancy_type', 'N/A'),
                  'vacancy': vacancy_info.get('vacancy', -1),
                  'lastupdate': vacancy_info.get('lastupdate', 'N/A')
              }]
          })

    result_dict[park_id] = {
        'park_id': park_id,
        'vehicle_type': vehicle_types
    }

  # Second URL transformation and merging
  for park in only_vacancy_data['car_park']:
    park_id = park['park_id']
    vehicle_types = park['vehicle_type']

    if park_id not in result_dict:
      result_dict[park_id] = {
          'park_id': park_id,
          'vehicle_type': []
      }

    result_dict[park_id]['vehicle_type'].extend(vehicle_types)


# Add basic info from the third URL
def add_basic_info(basic_info_data, result_dict):
  for park_info in basic_info_data['car_park']:
    park_id = park_info['park_id']

    if park_id in result_dict:
      result_dict[park_id].update(park_info)
    else:
      result_dict[park_id] = {
          **park_info,
          'park_id': park_id,
          'vehicle_type': []
      }


# Add more info from carpark-info-vacancy API
def add_more_info(result_dict):
  with ThreadPoolExecutor() as executor:
    futures = {
        executor.submit(requests.get, f'{carpark_info_url}&lang={lang}'): lang for lang in lang_versions
    }
    for future in as_completed(futures):
      lang = futures[future]
      carpark_info_res = future.result()
      carpark_info_data = carpark_info_res.json()

      for carpark_info in carpark_info_data['results']:
        park_id = carpark_info['park_Id']

        if park_id in result_dict:
          result_dict[park_id].setdefault('carpark_info_vacancy', {})[lang] = carpark_info
          
          # Convert park_Id to park_id
          result_dict[park_id] = {
            **result_dict[park_id],
            'carpark_info_vacancy': {
              **result_dict[park_id]['carpark_info_vacancy'],
              lang: {
                'park_id': park_id,
                **carpark_info,
              }
            }
          }
        else:
          result_dict[park_id] = {
              'park_id': park_id,
              'carpark_info_vacancy': {
                lang: carpark_info
              }
          }

        del result_dict[park_id]['carpark_info_vacancy'][lang]['park_Id']

# Fetch, transform, and return data
def fetch_and_transform():
  # Fetch the first URL and handle BOM by using utf-8-sig encoding
  carparkVacancyRes = requests.get(carpark_vacancy_url)
  carpark_vacancy_data = json.loads(
      carparkVacancyRes.content.decode('utf-8-sig'))

  # Fetch the second URL and handle BOM
  onlyVacancyRes = requests.get(only_vacancy_url)
  only_vacancy_data = json.loads(onlyVacancyRes.content.decode('utf-8-sig'))

  # Fetch additional basic information
  basicInfoRes = requests.get(basic_info_url)
  basic_info_data = json.loads(basicInfoRes.content.decode('utf-8-sig'))

  # Dictionary to hold the merged results
  result_dict = {}

  # Merge and transform the data
  merge_both_vacancy_api(carpark_vacancy_data, result_dict, only_vacancy_data)
  add_basic_info(basic_info_data, result_dict)

  # Now ensure that all language-specific data is fetched and added to result_dict
  add_more_info(result_dict)

  return list(result_dict.values())


# Fetch, transform, and write to file
def main():
  transformed_data = fetch_and_transform()

  # Write the transformed data to a JSON file
  with open(os.path.join(output_dir, file_name), 'w', encoding='utf-8') as f:
    json.dump(transformed_data, f, ensure_ascii=False, indent=None)
    print(f'Filtered data has been written to {file_name}')


# Run the main function
if __name__ == "__main__":
  main()
