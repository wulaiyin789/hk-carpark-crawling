import axios from 'axios';
import fs, { existsSync, mkdirSync } from 'node:fs';
import path from 'node:path';

const basicInfo = 'https://resource.data.one.gov.hk/td/carpark/basic_info_all.json';
const carparkInfo = 'https://api.data.gov.hk/v1/carpark-info-vacancy?data=info';
const carparkVacancy = 'https://api.data.gov.hk/v1/carpark-info-vacancy?data=vacancy';
const onlyVacancy = 'https://resource.data.one.gov.hk/td/carpark/vacancy_all.json';

const lang_versions = ['en_US', 'zh_TW', 'zh_CN'];

const output_dir = 'dist';
const fileName = 'carpark_data.json';

if (!existsSync(output_dir)) {
  mkdirSync(output_dir, { recursive: true });
}

// Merge two vacancies results into one
async function mergeBothVacancyApi(
  carparkVacancyData: { results: any },
  resultDict: any,
  onlyVacancyData: { car_park: ParkInfo[] }
) {
  // First URL transformation
  carparkVacancyData.results.forEach((park: any) => {
    const park_id = park['park_Id'];
    const vehicleTypes: VehicleType[] = [];

    // Dynamically handle all vehicle types present in park
    Object.keys(park).forEach((vehicleType) => {
      if (vehicleType !== 'park_Id') {
        park[vehicleType].forEach((vacancyInfo: ServiceCategory) => {
          vehicleTypes.push({
            type: vehicleType,
            service_category: [
              {
                category: 'HOURLY', // Assuming "HOURLY" as the category
                vacancy_type: vacancyInfo.vacancy_type || 'N/A',
                vacancy: vacancyInfo.vacancy || -1,
                lastupdate: vacancyInfo.lastupdate || 'N/A'
              }
            ]
          });
        });
      }
    });

    resultDict[park_id] = {
      park_id: park_id,
      vehicle_type: vehicleTypes
    };
  });

  // Second URL transformation and merging
  onlyVacancyData.car_park.forEach((park: ParkInfo) => {
    const park_id = park['park_id'];
    const vehicleTypes = park['vehicle_type'];

    if (!resultDict[park_id]) {
      resultDict[park_id] = {
        park_id: park_id,
        vehicle_type: []
      };
    }

    resultDict[park_id].vehicle_type.push(...vehicleTypes);
  });
}

// Add basic info from the third URL
async function addBasicInfo(basicInfoData: { car_park: ParkInfo[] }, resultDict: any) {
  basicInfoData.car_park.forEach((parkInfo: ParkInfo) => {
    const park_id = parkInfo['park_id'];

    if (resultDict[park_id]) {
      Object.assign(resultDict[park_id], parkInfo);
    } else {
      resultDict[park_id] = {
        park_id: park_id,
        vehicle_type: [],
        ...parkInfo
      };
    }
  });
}

// Add more info from carpark-info-vacancy API
async function addMoreInfo(resultDict: any) {
  for (const lang of lang_versions) {
    const carparkInfoRes = await axios.get(`${carparkInfo}&lang=${lang}`, { responseType: 'json' });
    const carparkInfoData = carparkInfoRes.data;

    carparkInfoData.results.forEach((carpark_info: any) => {
      const park_id = carpark_info['park_Id'];
      resultDict[park_id]['carpark_info_vacancy'] = {
        ...resultDict[park_id]['carpark_info_vacancy']
      };

      if (park_id === resultDict[park_id]['park_id']) {
        resultDict[park_id]['carpark_info_vacancy'] = {
          ...resultDict[park_id]['carpark_info_vacancy'],
          [lang]: {
            park_id: park_id,
            ...carpark_info
          }
        };
      } else {
        resultDict[park_id]['carpark_info_vacancy'] = {
          ...resultDict[park_id]['carpark_info_vacancy']
        };
      }

      delete resultDict[park_id]['carpark_info_vacancy'][lang]['park_Id'];

      console.log(`Added ${lang} to ${park_id}`);
    });
  }
}

async function fetchAndTransform(): Promise<ParkInfo[]> {
  // Fetch the data from all three URLs
  const [carparkVacancyRes, onlyVacancyRes, basicInfoRes] = await Promise.all([
    axios.get(carparkVacancy, { responseType: 'json' }),
    axios.get(onlyVacancy, { responseType: 'json' }),
    axios.get(basicInfo, { responseType: 'json' })
  ]);

  const carparkVacancyData = carparkVacancyRes.data;
  const onlyVacancyData = onlyVacancyRes.data;
  const basicInfoData = basicInfoRes.data;

  const resultDict: Record<string, ParkInfo> = {};

  await mergeBothVacancyApi(carparkVacancyData, resultDict, onlyVacancyData);

  await addBasicInfo(basicInfoData, resultDict);

  await addMoreInfo(resultDict);

  return Object.values(resultDict);
}

// Fetch, transform and write to file
fetchAndTransform().then((transformedData) => {
  // Write the data to a JSON file
  fs.writeFile(path.join(output_dir, fileName), JSON.stringify(transformedData, null, 0), (err) => {
    if (err) {
      console.error('Error writing to file', err);
    } else {
      console.log(`Data has been transformed to ${fileName}`);
    }
  });
});
