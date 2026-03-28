CALL upload_csv_to_table('/data/mock1.csv');
CALL upload_csv_to_table('/data/mock2.csv');
CALL upload_csv_to_table('/data/mock3.csv');
CALL upload_csv_to_table('/data/mock4.csv');
CALL upload_csv_to_table('/data/mock5.csv');
CALL upload_csv_to_table('/data/mock6.csv');
CALL upload_csv_to_table('/data/mock7.csv');
CALL upload_csv_to_table('/data/mock8.csv');
CALL upload_csv_to_table('/data/mock9.csv');
CALL upload_csv_to_table('/data/mock10.csv');

CALL process_pets();

CALL process_customers();

CALL process_sellers();

CALL process_suppliers();

SELECT reindex(1000);

CALL process_stores();

CALL process_products();

CALL process_sales();
