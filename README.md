# btc-blockchain-scanner
Load Bitcoin blockchain into MySQL DB

In order to load Bitcoin blockchain into MySQL DB you need:
1. Run local Bitcoin Core with RPC access enabled
2. Create MySQL DB and create table structure. Script is available at /src/main/resources/
3. Build this maven project (mvn clean install)
4. Run java -jar target/btc-scanner.jar update [options] (see run-update.cmd as an example)

The process takes few days. You can significantly reduce time (down to 8-12 hours) if you prepare full blocks in advance (command: prepare_blocks). 
Numbers are based on todays (Sep 2018) blockchain size of 212GB. Output DB size (MyISAM) with indexes ~145GB. Process ran on system with fast M2 SSD and Intel 10-cores CPU.

Destination DB structure:
<pre>
  block            (~541K records)
    transaction    (~342M)
      input        (~875M)
      output       (~931M)
        address_*  (~430M)
          wallet   (~150M)
</pre>

In order to minimize DB size signatures are not stored.


