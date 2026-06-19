set -e

# argument: i.e., `bash download_data.sh yellow 2020`
TAXI_TYPE=$1 # "yellow"
YEAR=$2 # 2020

if [ -z "${TAXI_TYPE}" ] || [ -z "${YEAR}" ]; then
  echo "usage: $0 <taxi_type> <year>   e.g. $0 yellow 2020" >&2
  exit 1
fi

URL_PREFIX="https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

for MONTH in {1..12}; do
  FMONTH=`printf "%02d" ${MONTH}`
  
  URL="${URL_PREFIX}/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.csv.gz"

LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  curl -L --fail "${URL}" -o "${LOCAL_PATH}"

done