DB_NAME="postgres"
DB_USER="venerabd"

cd "$(dirname "$0")/.."


psql -U $DB_USER -d $DB_NAME -f scripts/load_data_psql.sql

if [ $? -eq 0 ]; then
    echo "Data loaded successfully!"
else
    echo "Error loading data"
fi
