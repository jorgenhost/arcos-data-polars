#!/usr/bin/env -S uv run --script

uv sync
mkdir data
mkdir data/raw
curl -L -o data/raw/arcos-full.csv.gz "https://arcos-fulldataset.s3.amazonaws.com/arcos-fulldataset-2006-2019.csv.gz"

gzip -dk data/raw/arcos-full.csv.gz
uv run src/00_parse_data.py
