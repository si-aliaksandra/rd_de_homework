import json
import os
from config import Config
import requests
from requests.exceptions import RequestException


def main():

    config = Config("./config.yaml").get_config()

    url = config["url"]
    path_to_dir = os.path.join('.', 'data', config['API']['payload']['date'])
    os.makedirs(path_to_dir, exist_ok=True)

    try:
        request_token = requests.post(url + config['auth']['endpoint'], headers=config['auth']['headers'],
                                      data=json.dumps(config['auth']['payload']))
        token = request_token.json()['access_token']
        print(token)
        headers = config['API']['headers']
        headers['authorization'] = headers['authorization'].format(token=token)
        # OR headers = {"content-type": "application/json", "authorization": "JWT " + token}
        response = requests.get(url + config['API']['endpoint'], headers=headers,
                                data=json.dumps(config['API']["payload"]))
        response.raise_for_status()
        with open(os.path.join(path_to_dir, config['API']['payload']['date']), 'w') as json_file:
            json.dump(response.json(), json_file)
    except RequestException:
        print("Connection Error")


if __name__ == '__main__':
    main()
