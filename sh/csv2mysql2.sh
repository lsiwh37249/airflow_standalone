#!/bin/bash

CSV_PATH=$1

user="root"
password="qwer123"
database="history_db"

mysql --local-infile=1 -u"$user" -p"$password" "$database" <<EOF
LOAD DATA LOCAL INFILE '/home/kim1/data/csv/20240718/csv.csv'
INTO TABLE history_db.cmd_usage
CHARACTER SET latin1
FIELDS
	TERMINATED BY ','
	ESCAPED BY '\b'
	ESCAPED BY '\('
	ESCAPED BY '\)'
	ENCLOSED BY '^'
LINES TERMINATED BY '\n';
EOF
