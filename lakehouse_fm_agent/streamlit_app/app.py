# streamlit_app/app.py (top of file)
import os, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent  # repo root
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
