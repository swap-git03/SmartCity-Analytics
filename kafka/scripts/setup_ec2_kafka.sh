#!/bin/bash
# ==============================================================================
# Automated Apache Kafka & ZooKeeper Installation Script for Ubuntu 22.04 LTS
# Project: Smart City Traffic & Environmental Analytics Platform
# ==============================================================================

set -e

# Prompt for EC2 Public IP if not passed as argument
EC2_PUBLIC_IP=$1
if [ -z "$EC2_PUBLIC_IP" ]; then
    echo "[ERROR] Please provide your EC2 Public IP address as an argument!"
    echo "Usage: ./setup_ec2_kafka.sh <YOUR_EC2_PUBLIC_IP>"
    exit 1
fi

echo "=================================================================="
echo "Starting Apache Kafka Installation on EC2 Public IP: $EC2_PUBLIC_IP"
echo "=================================================================="

# 1. Update System & Install Java 17 OpenJDK
echo "[1/6] Updating APT repositories & installing Java OpenJDK 17..."
sudo apt update -y
sudo apt install -y openjdk-17-jdk wget curl net-tools tar

echo "Java Version:"
java -version

# 2. Download and Extract Apache Kafka 3.6.1 (Scala 2.13)
echo "[2/6] Downloading Apache Kafka 3.6.1..."
KAFKA_VERSION="3.6.1"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"

cd /home/ubuntu
if [ ! -d "$KAFKA_DIR" ]; then
    wget -q https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_DIR}.tgz
    tar -xzf ${KAFKA_DIR}.tgz
    rm ${KAFKA_DIR}.tgz
fi

sudo ln -sfn /home/ubuntu/${KAFKA_DIR} /opt/kafka
sudo chown -R ubuntu:ubuntu /opt/kafka /home/ubuntu/${KAFKA_DIR}

# 3. Configure Kafka Server Properties (listeners & advertised.listeners)
echo "[3/6] Configuring Kafka server.properties..."
SERVER_PROPS="/opt/kafka/config/server.properties"

# Backup original configuration
cp $SERVER_PROPS ${SERVER_PROPS}.bak

# Update Advertised Listeners to allow external traffic over Port 9092
sed -i "s/#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/0.0.0.0:9092/" $SERVER_PROPS
sed -i "s/#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/advertised.listeners=PLAINTEXT:\/\/${EC2_PUBLIC_IP}:9092/" $SERVER_PROPS

# 4. Create Systemd Service for ZooKeeper
echo "[4/6] Creating ZooKeeper systemd service..."
sudo bash -c 'cat <<EOF > /etc/systemd/system/zookeeper.service
[Unit]
Description=Apache ZooKeeper service
After=network.target

[Service]
Type=simple
User=ubuntu
ExecStart=/opt/kafka/bin/zookeeper-server-start.sh /opt/kafka/config/zookeeper.properties
ExecStop=/opt/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF'

# 5. Create Systemd Service for Kafka Broker
echo "[5/6] Creating Kafka Broker systemd service..."
sudo bash -c 'cat <<EOF > /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka Broker service
After=network.target zookeeper.service

[Service]
Type=simple
User=ubuntu
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF'

# 6. Reload Systemd, Start Services, & Create Topics
echo "[6/6] Reloading systemd, starting ZooKeeper and Kafka..."
sudo systemctl daemon-reload
sudo systemctl enable zookeeper
sudo systemctl start zookeeper

sleep 5

sudo systemctl enable kafka
sudo systemctl start kafka

sleep 10

echo "Checking ZooKeeper Status:"
sudo systemctl status zookeeper --no-pager | head -n 10

echo "Checking Kafka Status:"
sudo systemctl status kafka --no-pager | head -n 10

# Create Smart City Kafka Topics
echo "Creating Kafka Topics: traffic-raw-events, weather-raw-events, aqi-raw-events..."
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic traffic-raw-events
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic weather-raw-events
/opt/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic aqi-raw-events

echo "Listing Created Topics:"
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

echo "=================================================================="
echo "SUCCESS! Apache Kafka & ZooKeeper are running on EC2 ($EC2_PUBLIC_IP)"
echo "Kafka Listener: $EC2_PUBLIC_IP:9092"
echo "=================================================================="
