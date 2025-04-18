import requests
import os


def download(taxi, year, month):

    url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year}-{month}.parquet'

    response = requests.get(url)

    status_code = response.status_code

    if status_code != 200 :
        print(f"Problem connecting to url {url}, status code {status_code}.")
        return ''

    print(f'Downloading {url}')
    with open(f'./data/{year}/{taxi}_tripdata_{year}-{month}.parquet', mode="wb") as file:
        file.write(response.content)



# testing
if  __name__ == "__main__" :

    year = ['2022','2021','2020']#'2025','2024','2023']#,
    month = ['01','02','03','04','05','06','07','08','09','10','11','12']
    taxi = ['yellow']

    for y in year :
        print(f'year {y}')
        for m in month :
            print(f'month {m}')
            path = f'./data/{y}/{taxi[0]}_tripdata_{y}-{m}.parquet'
            if os.path.isfile(path) :
                print(f'{path} exists. Removing it.')
                os.remove(path)
        if os.path.exists(f'./data/{y}/') :
            print(f'Directory ./data/{y}/ exists. Removing it.')
            os.rmdir(f'./data/{y}')
        
    for y in year :
        os.mkdir(f'./data/{y}')
        for m in month :
            download(taxi[0],y,m)

    