import requests
import os

# Параметры Mockaroo API
API_URL = 'https://api.mockaroo.com/api/07cd64d0'
API_KEY = 'ff0eaa50'
NUM_RECORDS = 1000  # Количество записей для генерации

def fetch_mockaroo_data(api_url, api_key, num_records):
    url = f"{api_url}?count={num_records}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.content.decode('utf-8')
    else:
        response.raise_for_status()

def save_to_file(data, file_path):
    with open(file_path, 'w') as file:
        file.write(data)

def main():
    # Получение данных с Mockaroo
    data = fetch_mockaroo_data(API_URL, API_KEY, NUM_RECORDS)

    # Сохранение данных в папку /mnt/input
    output_path = '/home/petr0vsk/WorkSQL/Diplom/input/fake_data.csv'
    save_to_file(data, output_path)
    print(f'Data saved to {output_path}')

if __name__ == '__main__':
    main()
