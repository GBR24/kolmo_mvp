import yaml, os

def load_config(path="configs/kolmo.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

CONFIG = load_config()
