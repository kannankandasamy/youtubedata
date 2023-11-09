import configparser

def get_data_config():
    conf = {}
    cfg = configparser.ConfigParser()
    cfg.read('data.conf')
    for (key,val) in cfg.items('DATA_APP_CONFIGS'):
        conf[key]=val
    return conf