#!/bin/bash
make build
mkdir -p /usr/local/bin/quacfka-service
cp ./quacfka-service /usr/local/bin/quacfka-service/
mkdir -p /usr/local/bin/quacfka-service/parquet
mkdir -p /usr/local/bin/quacfka-service/config
cp ./config/server.toml /usr/local/bin/quacfka-service/config/
systemctl stop quacfka-service.service
systemctl disable quacfka-service.service
cp quacfka-service.service /lib/systemd/system/quacfka-service.service
systemctl enable quacfka-service.service && systemctl start quacfka-service.service