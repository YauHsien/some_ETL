import yaml, re

def to_yaml(json_data, key):
    if key is None:
        return yaml.dump(json_data,
                         default_flow_style= False).rstrip()
    else:
        m = re.match('(\w+)[.](\w+)', key)
        if m is None:
            key1 = key.upper()
            if key1 not in json_data:
                return None
            else:
                return yaml.dump({ key1: json_data[key1] },
                                 default_flow_style= False).rstrip()
        else:
            key1, key2 = m.group(1).upper(), m.group(2).upper()
            if key1 in json_data and key2 in json_data[key1]:
                return yaml.dump({ key1: { key2: json_data[key1][key2] }},
                                 default_flow_style= False).rstrip()
            else:
                return None
