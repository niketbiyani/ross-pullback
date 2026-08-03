#!/bin/bash
set -e

echo "Setting up HTTPS for Nginx..."

# 1. Create SSL directory for Nginx if not exists
sudo mkdir -p /etc/nginx/ssl

# 2. Copy the generated certs from the current directory
if [ -f "cert.pem" ] && [ -f "key.pem" ]; then
    sudo cp cert.pem /etc/nginx/ssl/scanner.crt
    sudo cp key.pem /etc/nginx/ssl/scanner.key
    echo "Copied certificates to /etc/nginx/ssl/"
else
    echo "Error: cert.pem or key.pem not found in $(pwd)!"
    echo "Please run this script from the ~/ross-pullback directory where cert.pem exists."
    exit 1
fi

# 3. Backup current Nginx config
NGINX_CONF="/etc/nginx/sites-available/default"
if [ -f "$NGINX_CONF" ]; then
    sudo cp "$NGINX_CONF" "${NGINX_CONF}.bak"
    echo "Backed up current Nginx config to ${NGINX_CONF}.bak"
fi

# 4. Open port 443 (HTTPS) in UFW firewall
sudo ufw allow 443/tcp || true

# 5. Write new Nginx config supporting both HTTP and HTTPS
sudo tee "$NGINX_CONF" > /dev/null << 'EOF'
server {
    listen 80 default_server;
    listen [::]:80 default_server;
    server_name _;

    # Redirect all HTTP traffic to HTTPS
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl default_server;
    listen [::]:443 ssl default_server;
    server_name _;

    ssl_certificate /etc/nginx/ssl/scanner.crt;
    ssl_certificate_key /etc/nginx/ssl/scanner.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    location / {
        proxy_pass https://127.0.0.1:5051/;
        proxy_ssl_verify off;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /scanner/ {
        proxy_pass https://127.0.0.1:5051/;
        proxy_ssl_verify off;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

echo "Testing Nginx configuration..."
sudo nginx -t

echo "Reloading Nginx..."
sudo systemctl reload nginx

echo "HTTPS setup completed successfully!"
echo "You can now access your dashboard directly at: https://88.208.255.34/scanner/"
