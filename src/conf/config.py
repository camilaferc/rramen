'''
Created on Oct 5, 2019

@author: camila
'''

#!/usr/bin/python
from configparser import ConfigParser
from pathlib import Path
 
def config(filename= str(Path(__file__).resolve().parents[2]) + '/config.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read conf file
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