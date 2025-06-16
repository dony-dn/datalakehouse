#!/bin/bash

# create Admin user, you can read these values from env or anywhere else possible
superset fab create-admin --username "$SUPERSET_ADMIN_USER" --firstname Superset --lastname Admin --email "$SUPERSET_ADMIN_EMAIL" --password "$SUPERSET_ADMIN_PASS"

# Upgrading Superset metastore
superset db upgrade

# setup roles and permissions
superset superset init 

pip install sqlalchemy-dremio

# Starting server
/bin/sh -c /usr/bin/run-server.sh