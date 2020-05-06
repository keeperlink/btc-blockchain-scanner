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

import com.sliva.btc.scanner.db.DBConnection;
import com.sliva.btc.scanner.db.DbQueryAddress;
import com.sliva.btc.scanner.db.DbQueryAddressCombo;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.BJBlockHandler;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.bitcoinj.core.Address;

/**
 *
 * @author whost
 */
public class RunAddress {

    private final String[] args;
    private final DBConnection conn;
    private final DbQueryAddress queryAddress;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(prepOptions(), args);
        if (cmd.hasOption('h')) {
            printHelpAndExit();
        }
        new RunAddress(cmd, args).runProcess();
    }

    @SuppressWarnings("UseSpecificCatch")
    public RunAddress(CommandLine cmd, String[] args) {
        this.args = args;
        DBConnection.applyArguments(cmd);
        DBConnection c = null;
        DbQueryAddress qa = null;
        try {
            c = new DBConnection();
            qa = new DbQueryAddressCombo(c);
        } catch (Exception e) {
            e.printStackTrace();
        }
        conn = c;
        queryAddress = qa;

    }

    private void runProcess() {
        for (String s : args) {
            if (!s.startsWith("-")) {
                processAddress(s);
            }
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    private void processAddress(String a) {
        if (a.length() <= 10 && StringUtils.isNumeric(a)) {
            try {
                BtcAddress btcAddress = queryAddress.findByAddressId(Integer.parseInt(a));
                System.out.println("Address for DB ID " + a + " is: " + btcAddress.getBjAddress()
                        + ", type: " + btcAddress.getType() + ", hash: " + Hex.encodeHexString(btcAddress.getAddress(), true));
            } catch (Exception e) {
                System.out.println("Error processing " + a + ": " + e.getMessage());
            }
        } else if (a.matches("[0-9a-fA-F]*") && a.length() == 40 || a.length() == 64) {
            if (queryAddress != null) {
                try {
                    BtcAddress btcAddress = queryAddress.findByAddress(Hex.decodeHex(a));
                    if (btcAddress != null) {
                        System.out.println("Address for hash " + a + " is: " + btcAddress.getBjAddress()
                                + ", type: " + btcAddress.getType() + ", DB ID: " + btcAddress.getAddressId());
                        return;
                    }
                } catch (Exception e) {
                    //
                }
            }
            BtcAddress.getRealTypes().forEach((type) -> {
                if (a.length() == 40 ^ type == SrcAddressType.P2WSH) {
                    try {
                        Address adr = BJBlockHandler.getAddress(type, Hex.decodeHex(a));
                        System.out.println("Address for hash " + a + " and type " + type + " is: " + adr);
                    } catch (Exception e) {
                    }
                }
            });
        } else {
            Address adr = BJBlockHandler.getAddress(a);
            System.out.println("Address " + a + " hash: " + Hex.encodeHexString(adr.getHash(), true)
                    + " type: " + BtcAddress.getTypeFromAddress(a));
        }
    }

    private static void printHelpAndExit() {
        System.out.println("Convert addresses between string hash and DB ID formats");
        System.out.println("Available options:");
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("java <jar> " + Main.Command.update_wallets + " [options] address1 [ address2 ...]", prepOptions());
        System.exit(1);
    }

    private static Options prepOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Print help");
        DBConnection.addOptions(options);
        return options;
    }
}
