#!/bin/bash
set -e  # Exit on error


# Check if the virtual environment already exists
if [ -d "venv" ] ; then
    echo "Virtual environment already exists."
else
    echo "Setting up Python environment..."
    python -m venv venv
fi

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing requirements..."
pip install -r requirements.txt

echo "Python requirements installed."

echo "Installing jq..."
# Install jq based on OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    sudo apt update && sudo apt install -y jq
elif [[ "$OSTYPE" == "darwin"* ]]; then
    brew install jq
elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    choco install -y jq
else
    echo "Please install jq manually for your OS."
fi

echo "Requirements installed. Setting up environment variables..."

make env

echo "Setup complete!"