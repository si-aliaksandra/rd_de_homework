import json
import os
import requests
from requests.exceptions import RequestException

def main():
    url = "https://robot-dreams-de-api.herokuapp.com/auth"
    headers = {"content-type": "application/json"}
    data = {"username": "rd_dreams", "password": "djT6LasE"}
    r = requests.post(url, headers=headers, data=json.dumps(data))
    token = r.json()['access_token']  # token we can't save in config file as it expires

    print(r.text)


    url = "https://robot-dreams-de-api.herokuapp.com/out_of_stock"
    headers = {"content-type": "application/json", "authorization": "JWT " + token}
    data = {"date": "2021-10-08"}
    r = requests.get(url, headers=headers, data=json.dumps(data))
    print(r.json())

    path_to_dir = os.path.join('.', 'data', data["date"])
    os.makedirs(path_to_dir, exist_ok=True)

    try:
        response = requests.get(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
        resp_data = response.json()
        with open(os.path.join(path_to_dir, data['date']), 'w') as json_file:
            json.dump(resp_data, json_file)
    except RequestException:
        print("Connection Error")



if __name__ =='__main__':
    main()
