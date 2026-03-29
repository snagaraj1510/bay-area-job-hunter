"""
Root conftest.py — ensures the project root is on sys.path so that
'from src.<module> import ...' works when running pytest from the project root.
"""

import sys
import os

# Insert the project root at the front of sys.path if it is not already there.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
