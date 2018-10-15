java -Xmx2g -jar target/btc-scanner.jar load_neo4j --neo-config=/etc/neo3.conf --db-config=/etc/db.conf --stop-file=/tmp/btc-neo-loader3-stop --threads=8 --batch-size=20000
@rem  --records-back=1000000
pause
