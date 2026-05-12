#!/bin/bash
set -e

mkdir -p duckdb_amalg
cd duckdb_amalg

echo "Downloading DuckDB v1.1.1 source amalgamation..."
wget -qO libduckdb-src.zip https://github.com/duckdb/duckdb/releases/download/v1.1.1/libduckdb-src.zip

echo "Extracting..."
unzip -o libduckdb-src.zip duckdb.hpp duckdb.cpp
rm libduckdb-src.zip

echo "DuckDB amalgamation downloaded successfully."
