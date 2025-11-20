#!/bin/bash

PROJECT_DIR="/home/dekii2275/TrendDectection"
VENV_DIR="$PROJECT_DIR/venv"
SCRIPT="$PROJECT_DIR/scripts/crawl_new_for_nifi.py"

cd "$PROJECT_DIR"

# activate venv nếu có
source "$VENV_DIR/bin/activate"

python "$SCRIPT"
