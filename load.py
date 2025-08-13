import csv

# Функция загрузки данных из city_data
def load_city_data(file_path):
    city_data = []

    with open(file_path, mode='r', encoding='utf-8', newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            city_data.append({'eng_name': row[0].strip(), 'rus_name': row[1].strip(), 'url': row[2].strip()})
    return city_data

city_data = load_city_data('city_data.csv')
