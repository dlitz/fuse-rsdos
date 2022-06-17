#!/bin/bash
# dlitz 2022
set -eu

echo >&2 "Creating venv..."
python3 -m venv --system-site-packages venv
echo >&2 "Activating venv..."
. venv/bin/activate
echo >&2 "pip install -e ."
pip install -e .
