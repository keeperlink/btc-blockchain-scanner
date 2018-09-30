java -Xmx2g -jar target/btc-scanner.jar load_neo4j --neo-config=/etc/neo.conf --db-config=/etc/db.conf --stop-file=/tmp/btc-neo-loader-stop --threads=8 --batch-size=50000
@rem  --records-back=1000000
