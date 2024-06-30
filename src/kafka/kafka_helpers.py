def read_config():
    config = {}
    with open("client.properties") as f:
        for line in f:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config
