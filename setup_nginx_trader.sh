#!/bin/bash
set -e

echo "Configuring HTTPS for your active Trader dashboard..."

# 1. Remove gex-analyzer if requested
if [ -L "/etc/nginx/sites-enabled/gex-analyzer" ]; then
    sudo rm "/etc/nginx/sites-enabled/gex-analyzer"
    echo "Removed gex-analyzer configuration."
fi

# 2. Restore default sites-available config to original state so it's clean
if [ -f "/etc/nginx/sites-available/default.bak" ]; then
    sudo cp "/etc/nginx/sites-available/default.bak" "/etc/nginx/sites-available/default"
    echo "Restored original default config."
fi

# 3. Create SSL directory for Nginx if not exists
sudo mkdir -p /etc/nginx/ssl

# 4. Copy the generated certs from the current directory
if [ -f "cert.pem" ] && [ -f "key.pem" ]; then
    sudo cp cert.pem /etc/nginx/ssl/scanner.crt
    sudo cp key.pem /etc/nginx/ssl/scanner.key
    echo "Copied certificates to /etc/nginx/ssl/"
else
    echo "Error: cert.pem or key.pem not found in $(pwd)!"
    echo "Please run this script from the ~/ross-pullback directory where cert.pem exists."
    exit 1
fi

# 5. Open port 443 (HTTPS) in UFW firewall
sudo ufw allow 443/tcp || true

# 6. Backup current trader config
TRADER_CONF="/etc/nginx/sites-enabled/trader"
if [ -f "$TRADER_CONF" ]; then
    sudo cp "$TRADER_CONF" "${TRADER_CONF}.bak"
    echo "Backed up current Nginx trader config to ${TRADER_CONF}.bak"
fi

# 7. Write new trader config supporting HTTPS
sudo tee "$TRADER_CONF" > /dev/null << 'EOF'
server {
    listen 80;
    server_name _;
    # Redirect all HTTP traffic to HTTPS
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl;
    server_name _;

    ssl_certificate /etc/nginx/ssl/scanner.crt;
    ssl_certificate_key /etc/nginx/ssl/scanner.key;

    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;

    auth_basic "Trading Dashboard";
    auth_basic_user_file /etc/nginx/.htpasswd;

    location / {
        proxy_pass http://127.0.0.1:5555;
        proxy_set_header Host $host;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_http_version 1.1;
    }

    location /analyser/ {
        proxy_pass http://127.0.0.1:5556/;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Prefix /analyser;
        proxy_http_version 1.1;
    }

    location /scanner/ {
        proxy_pass https://127.0.0.1:5051/;
        proxy_ssl_verify off;
        proxy_set_header Host $host;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_buffering off;
        proxy_cache off;
        chunked_transfer_encoding on;
        proxy_read_timeout 3600;
    }
}

server {
    listen 8080;
    server_name 88.208.255.34;

    location / {
        proxy_pass http://127.0.0.1:5566;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
}
EOF

echo "Testing Nginx configuration..."
sudo nginx -t

echo "Reloading Nginx..."
sudo systemctl reload nginx

echo "HTTPS setup completed successfully!"
echo "You can now access your dashboard directly at: https://88.208.255.34/scanner/"
