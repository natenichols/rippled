#!/bin/sh

# usage: sudo -u postgres ./initdb.sh
psql -c "CREATE USER rippled"
psql -c "CREATE DATABASE rippled WITH OWNER = rippled"

