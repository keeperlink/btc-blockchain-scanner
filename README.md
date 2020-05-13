# btc-blockchain-scanner
Load Bitcoin blockchain into MySQL DB

In order to load Bitcoin blockchain into MySQL DB you need:
1. Run local Bitcoin Core with RPC access enabled
2. Create MySQL DB and create table structure. Script is available at /src/main/resources/
3. Build this maven project (mvn clean install)
4. Run java -jar target/btc-scanner.jar update [options] (see run-update.cmd as an example)

The process takes few days. You can significantly reduce time (down to 12-24 hours) if you prepare full blocks in advance (command: prepare_blocks). 
Numbers are based on todays (May 2020) blockchain size of ~300GB. Output DB size (MyISAM) with indexes ~180GB. Time estimate based on system with fast M2 SSD and 10-cores CPU.

Destination DB structure:
<pre>
  block            (~630K records)
    transaction    (~530M)
      input        (~1.3B)
      output       (~1.4B)
        address_*  (~650M)
</pre>

In order to minimize DB size scrypts are not stored.


