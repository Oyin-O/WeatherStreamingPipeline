import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# run the dashboard directly
import runpy
runpy.run_module('dashboard.streamlit_dashboard', run_name='__main__')