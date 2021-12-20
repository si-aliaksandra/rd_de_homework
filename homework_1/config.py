import yaml


class Config:
    def __init__(self, path):
        with open(path, 'r') as yaml_file:
            self.__config = yaml.safe_load(yaml_file)

    def get_config(self):
        return self.__config

    def set_config(self, __config, key, value):
        self.__config[key] = value
