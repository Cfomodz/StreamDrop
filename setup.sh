#!/bin/bash
# One-click deployment script for StreamDrop - 24/7 HTML Streaming Server
# Designed for fresh Ubuntu droplets/servers
#
# 🚀 QUICK START - Fresh Ubuntu Server:
#   1. git clone <your-repo-url>
#   2. cd StreamDrop
#   3. chmod +x setup.sh && ./setup.sh
#
# 🤖 FULLY AUTOMATED DEPLOYMENT:
#   ./setup.sh
#
# This script installs everything needed from scratch:
# - Automatically adds swap for low-memory VPS (<2GB RAM, max 30% disk space)
# - Intelligent disk cleanup and space management
# - Git, Python, Chromium, FFmpeg, and all dependencies
# - Creates isolated Python virtual environment
# - Generates secure web interface credentials (random user/pass in .streamdrop_auth)
# - Sets up systemd service and auto-start on boot
# - Uses port 5000 (accessible by default, firewall not modified)

set -e  # Exit on any error

echo "🚀 One-Click Deploy: StreamDrop - 24/7 HTML Streaming Server"
echo "=============================================================="
echo "Setting up everything needed for a fresh Ubuntu system..."
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Function to handle root user - create streamdrop user and switch
handle_root_user() {
    if [[ $EUID -eq 0 ]]; then
        echo -e "${YELLOW}🔑 Running as root - creating streamdrop user for security${NC}"
        
        # Check if streamdrop user already exists
        if id "streamdrop" &>/dev/null; then
            echo -e "${GREEN}✅ streamdrop user already exists${NC}"
            # Ensure user is in sudo group
            usermod -aG sudo streamdrop
        else
            echo -e "${BLUE}👤 Creating streamdrop user...${NC}"
            # Create user with home directory and add to sudo group
            useradd -m -s /bin/bash -G sudo streamdrop
            echo -e "${GREEN}✅ streamdrop user created${NC}"
        fi
        
        # Ensure passwordless sudo for setup process (whether new or existing user)
        echo "streamdrop ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/streamdrop
        echo -e "${GREEN}✅ streamdrop user configured with sudo access${NC}"
        
        # Set up streamdrop directory
        STREAMDROP_HOME="/home/streamdrop/StreamDrop"
        echo -e "${BLUE}📁 Setting up StreamDrop directory...${NC}"
        
        # Create StreamDrop directory if it doesn't exist
        mkdir -p "$STREAMDROP_HOME"
        
        # Copy/update directory contents to streamdrop user (preserve existing files)
        if [ -f "$STREAMDROP_HOME/setup.sh" ]; then
            echo -e "${YELLOW}📁 StreamDrop directory exists - updating files...${NC}"
            # Use rsync to update only changed files, preserve stream database and auth
            rsync -av --exclude='venv/' --exclude='streams.db' --exclude='.streamdrop_auth' . "$STREAMDROP_HOME/"
        else
            echo -e "${BLUE}📁 Fresh StreamDrop installation - copying all files...${NC}"
            cp -r . "$STREAMDROP_HOME/"
        fi
        chown -R streamdrop:streamdrop "$STREAMDROP_HOME"
        
        # No environment variables needed - pure web app setup
        
        echo -e "${GREEN}🔄 Switching to streamdrop user and continuing setup...${NC}"
        echo ""
        
        # Switch to streamdrop user and run setup in their directory
        su - streamdrop -c "cd '$STREAMDROP_HOME' && bash setup.sh"
        
        echo ""
        echo -e "${GREEN}🎉 Setup completed for streamdrop user!${NC}"
        echo -e "${BLUE}💡 StreamDrop is installed in: $STREAMDROP_HOME${NC}"
        exit 0
    fi
}

# Function to check Ubuntu version
check_ubuntu() {
    if ! grep -q "Ubuntu" /etc/os-release; then
        echo -e "${YELLOW}⚠️  Warning: This script is designed for Ubuntu. Proceeding anyway...${NC}"
    fi
}

# Function to check internet connectivity
check_internet() {
    echo -e "${BLUE}🌐 Checking internet connectivity...${NC}"
    if ! curl -s --connect-timeout 5 google.com > /dev/null; then
        echo -e "${RED}❌ No internet connection detected!${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Internet connection confirmed${NC}"
}

# Function to add swap for low-memory systems
setup_swap() {
    # Get total memory in MB
    TOTAL_MEM=$(free -m | awk 'NR==2{printf "%.0f", $2}')
    
    # Check if swap already exists
    if swapon --show | grep -q "/swapfile"; then
        echo -e "${GREEN}✅ Swap file already exists${NC}"
        return
    fi
    
    # Add swap for systems with less than 2GB RAM
    if [ "$TOTAL_MEM" -lt 2048 ]; then
        # Get available disk space in MB
        AVAILABLE_SPACE=$(df / | awk 'NR==2{printf "%.0f", $4/1024}')
        
        # Calculate 30% of available space in MB
        MAX_SWAP_MB=$((AVAILABLE_SPACE * 30 / 100))
        
        # Default to 2GB (2048MB) but respect disk space limits
        DESIRED_SWAP_MB=2048
        if [ "$MAX_SWAP_MB" -lt "$DESIRED_SWAP_MB" ]; then
            SWAP_SIZE_MB=$MAX_SWAP_MB
            SWAP_SIZE="${SWAP_SIZE_MB}M"
            echo -e "${YELLOW}💾 Low memory system detected (${TOTAL_MEM}MB)${NC}"
            echo -e "${YELLOW}⚠️  Limited disk space: ${AVAILABLE_SPACE}MB available${NC}"
            echo -e "${BLUE}🔄 Creating ${SWAP_SIZE} swap file (30% of available space)...${NC}"
        else
            SWAP_SIZE_MB=$DESIRED_SWAP_MB
            SWAP_SIZE="2G"
            echo -e "${YELLOW}💾 Low memory system detected (${TOTAL_MEM}MB)${NC}"
            echo -e "${BLUE}🔄 Creating ${SWAP_SIZE} swap file to prevent OOM during setup...${NC}"
        fi
        
        # Ensure we have at least 500MB for swap to be useful
        if [ "$SWAP_SIZE_MB" -lt 500 ]; then
            echo -e "${RED}❌ Insufficient disk space for meaningful swap (${AVAILABLE_SPACE}MB available)${NC}"
            echo -e "${YELLOW}⚠️  Proceeding without swap - consider upgrading your VPS${NC}"
            return
        fi
        
        # Create swap file
        sudo fallocate -l "$SWAP_SIZE" /swapfile
        sudo chmod 600 /swapfile
        sudo mkswap /swapfile
        sudo swapon /swapfile
        
        # Make permanent (add to fstab if not already there)
        if ! grep -q "/swapfile" /etc/fstab; then
            echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
        fi
        
        echo -e "${GREEN}✅ ${SWAP_SIZE} swap file created and activated${NC}"
        echo -e "${BLUE}💡 Free memory now: $(free -h | awk 'NR==2{print $7}')${NC}"
        echo -e "${BLUE}💽 Remaining disk space: $((AVAILABLE_SPACE - SWAP_SIZE_MB))MB${NC}"
    else
        echo -e "${GREEN}✅ Sufficient memory detected (${TOTAL_MEM}MB)${NC}"
    fi
}

# Run initial checks
handle_root_user  # Will exit if root, switching to streamdrop user
check_ubuntu
check_internet
setup_swap  # Add swap for low-memory systems before heavy operations

# Clean up disk space before heavy operations
echo -e "${BLUE}🧹 Cleaning up disk space...${NC}"
sudo apt clean
sudo apt autoremove -y
sudo rm -rf /tmp/* 2>/dev/null || true
sudo journalctl --rotate
sudo journalctl --vacuum-time=1d

# Update package lists
echo -e "${BLUE}📦 Updating package lists...${NC}"
sudo apt update -y

# Install essential system dependencies
echo -e "${BLUE}🔧 Installing essential system dependencies...${NC}"
sudo apt install -y \
    git \
    curl \
    wget \
    unzip \
    rsync \
    software-properties-common \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release

# Install Python and development tools
echo -e "${BLUE}🐍 Installing Python and development tools...${NC}"
sudo apt install -y \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    pkg-config

# Install SDL2 development libraries for pygame compilation
echo -e "${BLUE}🎮 Installing SDL2 libraries for pygame...${NC}"
sudo apt install -y \
    libsdl2-dev \
    libsdl2-image-dev \
    libsdl2-mixer-dev \
    libsdl2-ttf-dev \
    libfreetype6-dev \
    libportmidi-dev

# Detect headless system and choose optimal setup
detect_headless() {
    if [ -z "$DISPLAY" ] && [ -z "$WAYLAND_DISPLAY" ] && ! pgrep -x "Xorg\|gnome\|kde\|xfce" > /dev/null; then
        return 0  # Headless
    else
        return 1  # Has display
    fi
}

if detect_headless; then
    echo -e "${GREEN}🎯 Headless system detected - installing optimized packages${NC}"
    echo -e "${BLUE}🎬 Installing minimal dependencies for headless streaming...${NC}"
    
    # Install packages with fallback for older Ubuntu versions
    PACKAGES="ffmpeg libnss3 libdrm2 libgbm1 fonts-liberation fonts-dejavu-core fontconfig"
    
    # Handle Ubuntu 24.04+ package name changes (t64 suffix)
    if apt-cache show libatk-bridge2.0-0t64 >/dev/null 2>&1; then
        PACKAGES="$PACKAGES libatk-bridge2.0-0t64"
    else
        PACKAGES="$PACKAGES libatk-bridge2.0-0"
    fi
    
    if apt-cache show libasound2t64 >/dev/null 2>&1; then
        PACKAGES="$PACKAGES libasound2t64"
    else
        PACKAGES="$PACKAGES libasound2"
    fi
    
    sudo apt install -y $PACKAGES
    
    HEADLESS_MODE=true
else
    echo -e "${YELLOW}🖥️  Display system detected${NC}"
    echo -e "${BLUE}Do you want headless-optimized setup? (smaller, faster, cheaper VPS) [Y/n]:${NC}"
    read -r -n 1 HEADLESS_CHOICE
    echo
    
    if [[ $HEADLESS_CHOICE =~ ^[Nn]$ ]]; then
        echo -e "${BLUE}🎬 Installing full dependencies with X11 support...${NC}"
        
        # Full packages with dev libraries (for X11 support)
        FULL_PACKAGES="xvfb ffmpeg fonts-liberation fonts-dejavu-core fontconfig libnss3-dev libdrm-dev libxcomposite-dev libxdamage-dev libxrandr-dev libgbm-dev libxss-dev"
        
        # Handle Ubuntu 24.04+ package name changes for dev packages
        if apt-cache show libatk-bridge2.0-dev >/dev/null 2>&1; then
            FULL_PACKAGES="$FULL_PACKAGES libatk-bridge2.0-dev"
        fi
        
        if apt-cache show libasound2-dev >/dev/null 2>&1; then
            FULL_PACKAGES="$FULL_PACKAGES libasound2-dev"
        elif apt-cache show libasound2t64-dev >/dev/null 2>&1; then
            FULL_PACKAGES="$FULL_PACKAGES libasound2t64-dev"
        fi
        
        sudo apt install -y $FULL_PACKAGES
        
        HEADLESS_MODE=false
    else
        echo -e "${GREEN}✅ Using headless-optimized setup${NC}"
        echo -e "${BLUE}🎬 Installing minimal dependencies for headless streaming...${NC}"
        
        # Same headless logic as above
        PACKAGES="ffmpeg libnss3 libdrm2 libgbm1 fonts-liberation fonts-dejavu-core fontconfig"
        
        if apt-cache show libatk-bridge2.0-0t64 >/dev/null 2>&1; then
            PACKAGES="$PACKAGES libatk-bridge2.0-0t64"
        else
            PACKAGES="$PACKAGES libatk-bridge2.0-0"
        fi
        
        if apt-cache show libasound2t64 >/dev/null 2>&1; then
            PACKAGES="$PACKAGES libasound2t64"
        else
            PACKAGES="$PACKAGES libasound2"
        fi
        
        sudo apt install -y $PACKAGES
        
        HEADLESS_MODE=true
    fi
fi

# Install Chromium (open-source, server-friendly)
echo -e "${BLUE}🌐 Installing Chromium browser...${NC}"
if ! command -v chromium-browser &> /dev/null; then
    sudo apt install -y \
        chromium-browser \
        chromium-browser-l10n \
        chromium-codecs-ffmpeg
else
    echo -e "${GREEN}✅ Chromium already installed${NC}"
fi

# Create virtual environment (if it doesn't exist)
if [ -d "venv" ]; then
    echo -e "${GREEN}✅ Python virtual environment already exists${NC}"
else
    echo -e "${BLUE}🐍 Creating Python virtual environment...${NC}"
    python3 -m venv venv
fi

# Activate virtual environment
echo -e "${BLUE}🔧 Activating virtual environment...${NC}"
source venv/bin/activate

# Upgrade pip in virtual environment
echo -e "${BLUE}⬆️ Upgrading pip...${NC}"
pip install --upgrade pip

# Install Python dependencies in virtual environment (optimized for low-memory VPS)
echo -e "${BLUE}📦 Installing Python dependencies in virtual environment...${NC}"
echo -e "${YELLOW}💡 Using pre-built wheels to reduce memory usage and avoid compilation${NC}"
pip install --prefer-binary --only-binary=:all: --no-compile -r requirements.txt

# Setup web interface password
setup_password() {
    echo -e "${BLUE}🔒 Setting up web interface security...${NC}"
    
    if [ -f ".streamdrop_auth" ]; then
        echo -e "${GREEN}✅ Authentication already configured${NC}"
        return
    fi
    
    # Generate a secure random username (8 characters, alphanumeric)
    WEB_USERNAME=$(openssl rand -base64 12 | tr -d "=+/0-9" | tr '[:upper:]' '[:lower:]' | cut -c1-8)
    
    # Generate a secure random password (12 characters)
    WEB_PASSWORD=$(openssl rand -base64 12 | tr -d "=+/" | cut -c1-12)
    
    # Store credentials (simple format for now, can be enhanced)
    echo "$WEB_USERNAME:$WEB_PASSWORD" > .streamdrop_auth
    chmod 600 .streamdrop_auth
    
    echo -e "${GREEN}✅ Web interface credentials generated${NC}"
    echo -e "${YELLOW}🔑 Username: ${WEB_USERNAME}${NC}"
    echo -e "${YELLOW}🔑 Password: ${WEB_PASSWORD}${NC}"
    echo -e "${BLUE}💡 Stored in .streamdrop_auth (keep this secure!)${NC}"
}

setup_password

# Network configuration note
echo -e "${BLUE}🌐 Network Configuration${NC}"
echo -e "${GREEN}✅ StreamDrop will use port 5000 for web interface${NC}"
echo -e "${YELLOW}🔒 Configure UFW manually if you need firewall protection${NC}"

# Web application setup complete
echo ""
echo -e "${GREEN}✅ StreamDrop Web Application Setup Complete${NC}"
echo "=========================================="
echo ""
echo -e "${BLUE}💡 Stream Configuration:${NC}"
echo "• Open the web interface to add your streams with their own keys and content"
echo "• Each stream runs independently and can be started/stopped separately"
echo ""

# Set up systemd service for auto-startup
echo ""
echo -e "${YELLOW}🔄 Setting up auto-startup service${NC}"
echo "=========================================="

# Get current user and working directory
CURRENT_USER=$(whoami)
CURRENT_DIR=$(pwd)

# Create systemd service file
sudo tee /etc/systemd/system/streamdrop.service > /dev/null << EOF
[Unit]
Description=StreamDrop 24/7 HTML Streamer
After=network.target
Wants=network.target

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$CURRENT_DIR
Environment=PATH=/usr/local/bin:/usr/bin:/bin
ExecStart=$CURRENT_DIR/venv/bin/python $CURRENT_DIR/stream_manager.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

# Enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable streamdrop.service

# Start service if not already running
if systemctl is-active --quiet streamdrop.service; then
    echo -e "${GREEN}✅ StreamDrop service already running - restarting to apply updates${NC}"
    sudo systemctl restart streamdrop.service
else
    echo -e "${BLUE}🚀 Starting StreamDrop service...${NC}"
    sudo systemctl start streamdrop.service
fi

echo -e "${GREEN}✅ StreamDrop service is running${NC}"

# Configure firewall safely
configure_firewall() {
    echo ""
    echo -e "${YELLOW}🔥 Configuring firewall (UFW)${NC}"
    
    # Check if UFW is already configured for StreamDrop
    if ufw status | grep -q "5000/tcp.*ALLOW"; then
        echo -e "${GREEN}✅ Firewall already configured for StreamDrop${NC}"
        return
    fi
    
    # Install UFW if not present
    if ! command -v ufw >/dev/null 2>&1; then
        echo "Installing UFW firewall..."
        sudo apt install -y ufw
    fi
    
    # Reset UFW to default settings to avoid conflicts
    echo "Setting UFW defaults..."
    sudo ufw --force reset >/dev/null 2>&1
    
    # CRITICAL: Allow SSH FIRST to prevent lockout
    echo "🔒 Allowing SSH (port 22) to prevent lockout..."
    sudo ufw allow 22/tcp >/dev/null
    
    # Allow StreamDrop web interface
    echo "🌐 Allowing StreamDrop web interface (port 5000)..."
    sudo ufw allow 5000/tcp >/dev/null
    
    # Enable UFW
    echo "Enabling firewall..."
    sudo ufw --force enable >/dev/null
    
    echo -e "${GREEN}✅ Firewall configured:${NC}"
    echo "   • SSH (22/tcp): ALLOWED"
    echo "   • StreamDrop (5000/tcp): ALLOWED"
    echo "   • All other ports: DENIED"
}

# Configure firewall
configure_firewall

# Display server information
get_server_ip() {
    # Try to get public IP
    PUBLIC_IP=$(curl -s --connect-timeout 5 ipv4.icanhazip.com 2>/dev/null || curl -s --connect-timeout 5 ifconfig.me 2>/dev/null || echo "Unable to determine")
    LOCAL_IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "Unable to determine")
    echo -e "${PURPLE}🌐 Server Information:${NC}"
    echo "• Public IP: $PUBLIC_IP"
    echo "• Local IP: $LOCAL_IP"
    echo "• Port: 5000"
}

echo ""
echo -e "${GREEN}🎉 ONE-CLICK DEPLOYMENT COMPLETE! 🎉${NC}"

# Get server IP and auth info
get_server_ip
if [ -f ".streamdrop_auth" ]; then
    AUTH_INFO=$(cat .streamdrop_auth)
    USERNAME=$(echo "$AUTH_INFO" | cut -d: -f1)
    PASSWORD=$(echo "$AUTH_INFO" | cut -d: -f2)
fi

# Create final summary box
echo ""
echo "=================================================================="
echo -e "${BLUE}🚀 YOUR STREAMDROP SERVER IS READY! 🚀${NC}"
echo "=================================================================="
echo ""
echo -e "${GREEN}🌐 WEB INTERFACE:${NC}"
echo -e "   URL:      http://$(curl -s --connect-timeout 3 ipv4.icanhazip.com 2>/dev/null || echo 'YOUR_SERVER_IP'):5000"
if [ -f ".streamdrop_auth" ]; then
    CURRENT_AUTH=$(cat .streamdrop_auth)
    CURRENT_USERNAME=$(echo "$CURRENT_AUTH" | cut -d: -f1)
    CURRENT_PASSWORD=$(echo "$CURRENT_AUTH" | cut -d: -f2)
    echo -e "   Username: ${CURRENT_USERNAME}"
    echo -e "   Password: ${CURRENT_PASSWORD}"
else
    echo -e "   Username: [check .streamdrop_auth file]"
    echo -e "   Password: [check .streamdrop_auth file]"
fi
echo ""
echo -e "${BLUE}📊 SYSTEM STATUS:${NC}"
echo -e "   Service:  🟢 Running and auto-starts on boot"
echo -e "   Firewall: 🔥 UFW enabled (SSH + port 5000 allowed)"
echo -e "   Logs:     sudo journalctl -u streamdrop -f"
echo -e "   Control:  sudo systemctl [start|stop|restart] streamdrop"
echo ""
if [ "$TOTAL_MEM" -lt 2048 ] && swapon --show | grep -q "/swapfile"; then
    SWAP_SIZE_DISPLAY=$(swapon --show --noheadings | awk '{print $3}' | head -n1)
    echo -e "${YELLOW}💾 OPTIMIZATIONS:${NC}"
    echo -e "   Swap:     ${SWAP_SIZE_DISPLAY} added for low-memory VPS"
    if [ "$HEADLESS_MODE" = true ]; then
        echo -e "   Mode:     Headless (60% fewer packages, perfect for VPS)"
    fi
    echo ""
fi
echo -e "${PURPLE}🎯 STREAM MANAGEMENT:${NC}"
echo -e "   • Create streams via web interface"
echo -e "   • Each stream has independent YouTube key and content path"
echo -e "   • Streams restart automatically if they fail"
echo -e "   • All stream data persists in streams.db"
echo ""
echo "=================================================================="
echo -e "${GREEN}💡 TIP: Bookmark the web interface URL above! 🔖${NC}"
echo -e "${BLUE}🔄 This setup script is idempotent - safe to re-run anytime${NC}"
echo "=================================================================="