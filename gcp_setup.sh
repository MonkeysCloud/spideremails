#!/bin/bash
# GCP VM Setup Script for OpenVC Spider
# Run this on a fresh Ubuntu 22.04 VM

set -e

echo "=== Updating system ==="
sudo apt-get update && sudo apt-get upgrade -y

echo "=== Installing dependencies ==="
sudo apt-get install -y \
    wget curl unzip \
    python3 python3-pip python3-venv \
    xvfb \
    x11vnc \
    fluxbox \
    xterm \
    fonts-liberation \
    libnss3 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libasound2

echo "=== Installing Google Chrome ==="
wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb || sudo apt-get install -f -y
rm google-chrome-stable_current_amd64.deb

echo "=== Setting up Python environment ==="
python3 -m venv ~/spider-env
source ~/spider-env/bin/activate
pip install --upgrade pip
pip install playwright pandas aiohttp

echo "=== Installing Playwright browsers ==="
playwright install chromium

echo "=== Creating startup scripts ==="

# Script to start display and VNC
cat > ~/start_vnc.sh << 'EOF'
#!/bin/bash
# Start virtual display
export DISPLAY=:99
Xvfb :99 -screen 0 1920x1080x24 &
sleep 2

# Start window manager
fluxbox &
sleep 1

# Start VNC server (password: spider)
x11vnc -display :99 -forever -shared -rfbport 5900 -passwd spider &

echo "VNC server started on port 5900 (password: spider)"
echo "Connect with: ssh -L 5900:localhost:5900 YOUR_VM_IP"
EOF
chmod +x ~/start_vnc.sh

# Script to start Chrome
cat > ~/start_chrome.sh << 'EOF'
#!/bin/bash
export DISPLAY=:99
google-chrome \
    --remote-debugging-port=9222 \
    --no-first-run \
    --no-default-browser-check \
    --disable-gpu \
    --window-size=1920,1080 \
    --disable-dev-shm-usage \
    "https://www.openvc.app/search" &
echo "Chrome started with remote debugging on port 9222"
EOF
chmod +x ~/start_chrome.sh

# Script to run spider
cat > ~/run_spider.sh << 'EOF'
#!/bin/bash
source ~/spider-env/bin/activate
cd ~/spiderwebsites
python openvc_detailed_spider.py
EOF
chmod +x ~/run_spider.sh

echo ""
echo "=== Setup Complete ==="
echo ""
echo "NEXT STEPS:"
echo "1. Upload your spider files to ~/spiderwebsites/"
echo "2. Upload your openvc_detailed_results.csv to resume"
echo "3. Run: ~/start_vnc.sh"
echo "4. SSH tunnel: ssh -L 5900:localhost:5900 YOUR_VM_USER@YOUR_VM_IP"
echo "5. Connect VNC client to localhost:5900 (password: spider)"
echo "6. Run: ~/start_chrome.sh"
echo "7. Run: ~/run_spider.sh"
echo ""
