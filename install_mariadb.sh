#!/bin/bash

set -e

echo "Installing MariaDB..."
sudo pacman -S --noconfirm mariadb

echo "Initializing MariaDB data directory..."
sudo mariadb-install-db --user=mysql --basedir=/usr --datadir=/var/lib/mysql

echo "Starting MariaDB service..."
sudo systemctl start mariadb
sudo systemctl enable mariadb

echo "Waiting for MariaDB to be ready..."
sleep 3

echo "Creating user tomy.berrios with sysadmin privileges..."
sudo mariadb <<EOF
CREATE USER IF NOT EXISTS 'tomy.berrios'@'localhost' IDENTIFIED BY 'Yucaquemada1';
GRANT ALL PRIVILEGES ON *.* TO 'tomy.berrios'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
SELECT User, Host FROM mysql.user WHERE User='tomy.berrios';
EOF

echo "MariaDB installation complete!"
echo "User 'tomy.berrios' has been created with full administrative privileges."
echo "You can now connect using: mariadb -u tomy.berrios -p"
