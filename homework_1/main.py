import json
import os
from config import Config
import requests
from requests.exceptions import RequestException


def main():

    api_config = Config("./API.yaml").get_config()
    auth_config = Config("./auth.yaml").get_config()

    url = api_config["url"]

    path_to_dir = os.path.join('.', 'data', api_config["payload"]['date'])
    os.makedirs(path_to_dir, exist_ok=True)

    try:
        request_token = requests.post(url + auth_config["endpoint"], headers=auth_config["headers"],
                                      data=json.dumps(auth_config["payload"]))
        token = request_token.json()['access_token']
        print(token)
        headers = api_config["headers"]
        headers["authorization"] = headers["authorization"].format(token=token)
        # OR headers = {"content-type": "application/json", "authorization": "JWT " + token}
        response = requests.get(url + api_config["endpoint"], headers=headers,
                                data=json.dumps(api_config["payload"]))
        response.raise_for_status()
        with open(os.path.join(path_to_dir, api_config["payload"]['date']), 'w') as json_file:
            json.dump(response.json(), json_file)
    except RequestException:
        print("Connection Error")


if __name__ == '__main__':
    main()
