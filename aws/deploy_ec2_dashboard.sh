#!/bin/bash
# ==============================================================================
# UrbanPulse: Automated AWS EC2 Live Nginx Web Server Deployment
# Hosts the Smart City Operations Dashboard on AWS EC2 @ http://13.217.6.185
# ==============================================================================

set -e

echo "=== 1. Updating Ubuntu Packages & Installing Nginx ==="
sudo apt-get update -y
sudo apt-get install nginx -y

echo "=== 2. Setting Up Nginx Document Root ==="
sudo mkdir -p /var/www/urbanpulse
sudo cp -f powerbi/dashboard.html /var/www/urbanpulse/index.html

echo "=== 3. Configuring Nginx Virtual Host ==="
cat << 'EOF' | sudo tee /etc/nginx/sites-available/urbanpulse
server {
    listen 80 default_server;
    listen [::]:80 default_server;

    root /var/www/urbanpulse;
    index index.html;

    server_name _;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
EOF

sudo ln -sf /etc/nginx/sites-available/urbanpulse /etc/nginx/sites-enabled/default

echo "=== 4. Testing & Restarting Nginx Server ==="
sudo nginx -t
sudo systemctl restart nginx
sudo systemctl enable nginx

echo "=========================================================================="
echo " SUCCESS! UrbanPulse Live Control Room Dashboard Hosted on AWS EC2!"
echo " Public Access URL: http://13.217.6.185"
echo "=========================================================================="
