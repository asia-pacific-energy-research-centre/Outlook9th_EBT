

import pandas as pd 
import re
import os
from datetime import datetime

USE_SINGLE_ECONOMY = True
SINGLE_ECONOMY = '19_THA'

EBT_EARLIEST_YEAR = 1980 
OUTLOOK_BASE_YEAR = 2020
OUTLOOK_LAST_YEAR = 2070

SECTOR_LAYOUT_SHEET = 'sector_layout_20230330'
FUEL_LAYOUT_SHEET = 'fuel_layout_20230329'

def set_working_directory():
    # Change the working drive
    wanted_wd = 'Outlook9th_EBT'
    os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)

