"""Initialise configurations"""

import os
import yaml


try:
    with open("config.yaml", mode="r", encoding="utf-8") as stream:
        YAML_CONFIG = yaml.safe_load(stream)
except FileNotFoundError:
    YAML_CONFIG = None

with open("config.defaults.yaml", mode="r", encoding="utf-8") as stream:
    YAML_CONFIG_DEFAULTS = yaml.safe_load(stream)


def get_config(key: str) -> str:
    """Find config in env var/config.yaml"""
    value = os.environ.get(key)
    if not value and YAML_CONFIG:
        value = YAML_CONFIG.get(key)
    if not value:
        value = YAML_CONFIG_DEFAULTS[key]
    return value
