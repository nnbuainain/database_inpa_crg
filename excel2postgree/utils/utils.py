from configparser import ConfigParser
import sys, time, os, constant
import pandas as pd

def delete_last_line(num_rows, wait: int) -> None:
    cursor_up = '\x1b[1A'
    delete_line = '\x1b[2K'

    time.sleep(wait)

    for _ in range(num_rows):
        sys.stdout.write(cursor_up)
        sys.stdout.write(delete_line)

def cleanup() -> None:
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def config(filename='database.ini', section='postgresql') -> None:
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def read_file():
    df = pd.read_excel(constant.FILE_PATH)
    df.index = df.index + 1
    return df