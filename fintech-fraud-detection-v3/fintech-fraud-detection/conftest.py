"""
conftest.py — pytest root configuration.
Adds the project root to sys.path so that `from src.X import Y` works
both locally and in GitHub Actions CI without installing the package.
"""
import sys
import os

# Ensure the project root is on the Python path
sys.path.insert(0, os.path.dirname(__file__))
