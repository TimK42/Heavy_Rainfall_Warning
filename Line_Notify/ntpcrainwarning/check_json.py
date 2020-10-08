def read_json():
    f = open("json.txt", "r")
    json_text = f.read()
    f.close()
    return json_text


def write_json(json_text):
    f = open("json.txt", "w")
    f.write(json_text)
    f.close()
    return True


def read_time():
    f = open("time.txt", "r")
    time_text = f.read()
    f.close()
    return time_text


def write_time(time_text):
    f = open("time_text", "w")
    f.write(time_text)
    f.close()
    return True