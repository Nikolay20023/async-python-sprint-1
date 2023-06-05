from external.client import YandexWeatherAPI
from utils import get_url_by_city_name
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import  Process, Manager
import threading
from utils import CITIES
from external.analyzer import (
    INPUT_DAY_SUITABLE_CONDITIONS,
    INPUT_DAY_HOURS_START,
    INPUT_DAY_HOURS_END
)
from pydantic import BaseModel


class City(BaseModel):
    name = 's'
    temp_avg = 0
    hours_count = 0
    date = "sds"


class DataFetchingTask:

    def fetch_url(city_name: str) -> dict: 
        url_with_data = get_url_by_city_name(city_name)
        try:
            data = YandexWeatherAPI.get_forecasting(url_with_data)
            return data
        except KeyError:
            print(f'City: {city} missing "forecasts" ')


class DataCalculationTask:

    def to_calculate_temp(args: tuple('[dict, queue, str]')) -> dict:
        try:
            data, queue, city = args
            for date in data['forecasts']:
                sum_tem = 0
                clear_hour = 0
                for hour in date['hours'][
                    INPUT_DAY_HOURS_START: INPUT_DAY_HOURS_END
                ]:
                    sum_tem += hour['temp']
                    if hour['condition'] in INPUT_DAY_SUITABLE_CONDITIONS:
                        clear_hour += 1
                data = {
                    "name": city,
                    "date": date['date'],
                    "hours_count": clear_hour,
                    "temp_avg": sum_tem / 11
                }
                city_class = City(**data)
                queue.put(city_class)
        except KeyError:
            print(f'City: {city} hasnt forecast')
        except TypeError:
            print(f'City: {city} empty data')
            

class DataAggregationTask:
    
    def aggregate(args):
        queue, global_dict, lock = args
        while not queue.empty():
            with lock:
                item: dict = queue.get(block=False)
                if item is None:
                    break
                item_key = item.name
                if item_key in global_dict:
                    global_dict[item_key] = global_dict[item_key]
                    + list(item.dict().values())
                else:
                    global_dict[item_key] = []
                    global_dict[item_key] = global_dict[item_key]
                    + list(item.dict().values())


class DataAnalyzingTask:
    
    def analyse(args):
        data, list_city, lock = args
        with lock:
            for key, val in data.items():
                sum_temp, clear_days = 0, 0
                for _ in val:
                    sum_temp += _['temp_avg']
                    clear_days += _['hours_count']
                if sum_temp >= list_city[-1][-2] and clear_days >= list_city[-1][-1]:
                    list_city.append([key, sum_temp, clear_days])


if __name__ == "__main__":
    with ThreadPoolExecutor() as pool:
        data_citys_dict = {}   
        for city in CITIES:
            try:
                future = pool.submit(DataFetchingTask.fetch_url, city)
            except KeyError:
                print(f'This: {city} missing "forecasts".')
            data_citys_dict[city] = future.result()
    
    lock = threading.Lock()
    m = Manager()
    queue = m.Queue()
    global_dict = m.dict()
    processes = []

    for city, chunk  in data_citys_dict.items():
        pr_calculated = Process(
            target=DataCalculationTask.to_calculate_temp,
            args=[(chunk, queue, city)]
        )
        pr_calculated.start()
        processes.append(pr_calculated)
    for pr in processes:
        pr.join()
    
    with ThreadPoolExecutor() as pool:
        future = pool.submit(
            DataAggregationTask.aggregate,
            (queue, global_dict, lock)       
        )
    list_city = m.list()
    list_city.append(['city', 0, 0])

    processes = []

    for city, chunk  in global_dict.items():
        pr_calculated = Process(
            target=DataAnalyzingTask.analyse,
            args=[(global_dict, list_city, lock)]
        )
        pr_calculated.start()
        processes.append(pr_calculated)
    for pr in processes:
        pr.join()

    # with ThreadPoolExecutor() as executor:
    #     future = executor.submit(
    #         DataAnalyzingTask.analyse,
    #         (global_dict, list_city, lock)
    #     )
    print(f'Самый оптимальный город: {list_city[-1][0]}')

    