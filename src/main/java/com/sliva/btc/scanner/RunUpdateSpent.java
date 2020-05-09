/*
 * Copyright 2018 Sliva Co.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sliva.btc.scanner;

import com.sliva.btc.scanner.db.DBConnectionSupplier;
import com.sliva.btc.scanner.db.DBPreparedStatement;
import com.sliva.btc.scanner.db.DbUpdateOutput;
import com.sliva.btc.scanner.db.model.OutputStatus;
import com.sliva.btc.scanner.util.Utils;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunUpdateSpent {

    private static final int DEFAULT_START_TRANSACTION_ID = 0;
    private static final int DEFAULT_BATCH_SIZE = 200_000;
    private static final String SQL_QUERY_OUTPUTS
            = "SELECT O.transaction_id,O.pos,O.address_id,O.spent,I.in_transaction_id FROM output O"
            + " LEFT JOIN input I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos"
            + " WHERE O.transaction_id BETWEEN ? AND ?";

    private final DBConnectionSupplier dbCon;
    private final DBPreparedStatement psQueryOutputs;
    private final int startTransactionId;
    private final int batchSize;
    private final Utils.NumberFile startFromFile;

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws Exception {
        DbUpdateOutput.MAX_UPDATE_QUEUE_LENGTH = 50000;
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(prepOptions(), args);
        if (cmd.hasOption('h')) {
            printHelpAndExit();
        }
        log.info("START");
        try {
            new RunUpdateSpent(cmd).runProcess();
        } finally {
            log.info("FINISH");
        }
    }

    public RunUpdateSpent(CommandLine cmd) {
        startFromFile = new Utils.NumberFile(cmd.getOptionValue("start-from", Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        startTransactionId = startFromFile.getNumber().intValue();
        batchSize = Integer.parseInt(cmd.getOptionValue("batch-size", Integer.toString(DEFAULT_BATCH_SIZE)));
        DBConnectionSupplier.applyArguments(cmd);

        dbCon = new DBConnectionSupplier();
        psQueryOutputs = dbCon.prepareStatement(SQL_QUERY_OUTPUTS);
    }

    private void runProcess() throws SQLException {
        for (int i = startTransactionId;; i += batchSize) {
            log.info("Processing batch of outputs for transaction IDs between {} and {}", i, i + batchSize);
            startFromFile.updateNumber(i);
            psQueryOutputs.getParamSetter().setInt(i).setInt(i + batchSize).checkStateReady();
            int txnCount = 0;
            try (ResultSet rs = psQueryOutputs.executeQuery();
                    DbUpdateOutput updateOutput = new DbUpdateOutput(dbCon)) {
                while (rs.next()) {
                    txnCount++;
                    int transactionId = rs.getInt(1);
                    short pos = rs.getShort(2);
                    int addressId = rs.getInt(3);
                    int spent = rs.getInt(4);
                    Object input = rs.getObject(5);
                    if (input == null) {
                        if (addressId != 0 && spent != OutputStatus.UNSPENT) {
                            updateOutput.updateSpent(transactionId, pos, OutputStatus.UNSPENT);
                        } else if (addressId == 0 && spent < OutputStatus.UNSPENDABLE) {
                            updateOutput.updateSpent(transactionId, pos, OutputStatus.UNSPENDABLE);
                        }
                    } else if (spent != OutputStatus.SPENT) {
                        updateOutput.updateSpent(transactionId, pos, OutputStatus.SPENT);
                    }
                }
            }
            if (txnCount == 0) {
                log.info("Reached end of transactions table");
                break;
            }
        }
    }

    private static void printHelpAndExit() {
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + Main.Command.update_wallets + " [options]", prepOptions());
        System.exit(1);
    }

    private static Options prepOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        options.addOption(null, "batch-size", true, "Number or transactions to process in a batch. Default: " + DEFAULT_BATCH_SIZE);
        options.addOption(null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");
        DBConnectionSupplier.addOptions(options);
        return options;
    }

}
