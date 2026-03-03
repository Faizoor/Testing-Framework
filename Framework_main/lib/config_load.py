import yaml 

def config_loader(yaml_path):
    """
        Read the yaml file and returns it
    """
    with open(yaml_path,'r') as conf:
        return yaml.safe_load(conf)