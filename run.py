# -*- coding: utf-8 -*-
"""
Created on Thu Jun 15 13:54:43 2023
wrapper for cx_freeze executable of hercules dashboard
@author: David.Kenward
"""

import subprocess
import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import re
import sqlite3
import numpy as np

if __name__ == '__main__':
    subprocess.run('streamlit run hercules.py')