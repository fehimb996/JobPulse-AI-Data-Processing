#!/bin/bash
# startup.sh - Azure App Service startup script
echo "Starting Azure App Service deployment..."

# Set Python path and environment
export PYTHONPATH=/home/site/wwwroot:$PYTHONPATH
export FLASK_APP=app.py
export FLASK_ENV=production

# Navigate to application directory
cd /home/site/wwwroot

# Create a log file for debugging
exec > >(tee -a /home/site/wwwroot/startup.log) 2>&1

echo "Python version: $(python --version)"
echo "Current directory: $(pwd)"
echo "Directory contents: $(ls -la)"

# Check if virtual environment exists, if not create it
if [ ! -d "venv" ]; then
  echo "Creating virtual environment..."
  python -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip quickly
echo "Upgrading pip..."
python -m pip install --upgrade pip --quiet

# Install requirements first
echo "Installing requirements..."
pip install -r requirements.txt --quiet

# Install Playwright browsers (this is the key addition)
echo "Installing Playwright browsers..."
playwright install chromium --with-deps

# Verify Playwright installation
echo "Verifying Playwright installation..."
python -c "from playwright.sync_api import sync_playwright; print('Playwright imported successfully')"

# Start Flask application
echo "Starting Flask application..."
python app.py