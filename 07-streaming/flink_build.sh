# Script to download flink build files
PREFIX="https://raw.githubusercontent.com/DataTalksClub/data-engineering-zoomcamp/main/07-streaming/workshop"

curl -LO ${PREFIX}/Dockerfile.flink
curl -LO ${PREFIX}/pyproject.flink.toml
curl -LO ${PREFIX}/flink-config.yaml