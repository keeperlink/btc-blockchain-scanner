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
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildOption;
import com.sliva.btc.scanner.util.Utils;
import com.sliva.btc.scanner.util.Utils.NumberFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author Sliva Co
 */
@Slf4j
public class RunUpdateSpent {

    private static final int DEFAULT_START_TRANSACTION_ID = 0;
    private static final int DEFAULT_BATCH_SIZE = 200_000;

    public static final Collection<CommandLineUtils.CmdOption> CMD_OPTS = new ArrayList<>();
    private static final CommandLineUtils.CmdOption batchSizeOpt = buildOption(CMD_OPTS, null, "batch-size", true, "Number or transactions to process in a batch. Default: " + DEFAULT_BATCH_SIZE);
    private static final CommandLineUtils.CmdOption startFromOpt = buildOption(CMD_OPTS, null, "start-from", true, "Start process from this transaction ID. Beside a number this parameter can be set to a file name that stores the numeric value updated on every batch");

    static {
        CMD_OPTS.addAll(DBConnectionSupplier.CMD_OPTS);
    }

    private static final String SQL_QUERY_OUTPUTS
            = "SELECT O.transaction_id,O.pos,O.address_id,O.spent,I.in_transaction_id FROM output O"
            + " LEFT JOIN input I ON I.in_transaction_id=O.transaction_id AND I.in_pos=O.pos"
            + " WHERE O.transaction_id BETWEEN ? AND ?";

    private final DBConnectionSupplier dbCon;
    private final DBPreparedStatement psQueryOutputs;
    private final int startTransactionId;
    private final int batchSize;
    private final NumberFile startFromFile;

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     */
    public static void main(String[] args) throws Exception {
        DbUpdateOutput.MAX_UPDATE_QUEUE_LENGTH = 50000;
        CmdArguments cmd = CommandLineUtils.buildCmdArguments(args, Main.Command.update_spent.name(), CMD_OPTS);
        log.info("START");
        try {
            new RunUpdateSpent(cmd).runProcess();
        } finally {
            log.info("FINISH");
        }
    }

    public RunUpdateSpent(CmdArguments cmd) {
        startFromFile = new Utils.NumberFile(cmd.getOption(startFromOpt).orElse(Integer.toString(DEFAULT_START_TRANSACTION_ID)));
        startTransactionId = startFromFile.getNumber().intValue();
        batchSize = cmd.getOption(batchSizeOpt).map(Integer::parseInt).orElse(DEFAULT_BATCH_SIZE);
        dbCon = new DBConnectionSupplier();
        psQueryOutputs = dbCon.prepareStatement(SQL_QUERY_OUTPUTS);
    }

    private void runProcess() throws SQLException {
        for (AtomicInteger i = new AtomicInteger(startTransactionId);; i.addAndGet(batchSize)) {
            log.info("Processing batch of outputs for transaction IDs between {} and {}", i.get(), i.get() + batchSize);
            startFromFile.updateNumber(i.get());
            try (DbUpdateOutput updateOutput = new DbUpdateOutput(dbCon)) {
                if (psQueryOutputs.setParameters(p -> p.setInt(i.get()).setInt(i.get() + batchSize)).executeQuery(rs -> {
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
                }) == 0) {
                    log.info("Reached end of transactions table");
                    break;
                }
            }
        }
    }
}
