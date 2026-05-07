"""
tests/conftest.py
──────────────────
Ensures the project root is on sys.path so that imports like
`from transformation.transform import ...` work when running pytest
from any working directory.
"""

import sys
import os

# Add the project root directory to Python's module search path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))