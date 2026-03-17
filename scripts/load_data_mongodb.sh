DB_NAME="big_data_assignment"
DATA_DIR="data/cleaned"

mongoimport --db $DB_NAME --collection campaigns --type csv --headerline --drop --file $DATA_DIR/campaigns.csv
mongoimport --db $DB_NAME --collection events --type csv --headerline --drop --file $DATA_DIR/events.csv
mongoimport --db $DB_NAME --collection friends --type csv --headerline --drop --file $DATA_DIR/friends.csv
mongoimport --db $DB_NAME --collection client_first_purchase_date --type csv --headerline --drop --file $DATA_DIR/client_first_purchase_date.csv
mongoimport --db $DB_NAME --collection messages --type csv --headerline --drop --file $DATA_DIR/messages.csv

echo "Data imported. Creating indexes"

mongosh --file scripts/load_data_mongodb.js

echo "MongoDB setup finished"
