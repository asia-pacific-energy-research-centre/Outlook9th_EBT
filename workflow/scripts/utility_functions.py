

import pandas as pd 
import re
import os
from datetime import datetime
import utility_functions as util

def set_working_directory():
    # Change the working drive
    wanted_wd = 'Outlook9th_EBT'
    os.chdir(re.split(wanted_wd, os.getcwd())[0] + wanted_wd)


def import_useful_varaibles():
    USE_SINGLE_ECONOMY = True
    single_economy = '19_THA'
    return USE_SINGLE_ECONOMY, single_economy