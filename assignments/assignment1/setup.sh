#!/bin/bash

# Step 1: Create virtual environment
python3 -m venv venv

# Step 2: Activate virtual environment
source venv/bin/activate

# Step 3: Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

echo "✅ Virtual environment is ready!"


# To run on terminal
# chmod +x setup.sh   # Make it executable
# ./setup.sh          # Run the setup script