java -Xmx2g -jar target/btc-scanner.jar load_neo4j --neo-config=/etc/neo2.conf --db-config=/etc/db.conf --stop-file=/tmp/btc-neo-loader2-stop --threads=4 --batch-size=20000
@rem  --records-back=100000
