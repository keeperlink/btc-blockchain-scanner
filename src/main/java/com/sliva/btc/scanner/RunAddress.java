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
import com.sliva.btc.scanner.db.DbQueryAddressOne;
import com.sliva.btc.scanner.db.DbQueryAddressCombo;
import com.sliva.btc.scanner.db.model.BtcAddress;
import com.sliva.btc.scanner.src.SrcAddressType;
import com.sliva.btc.scanner.util.BJBlockHandler;
import com.sliva.btc.scanner.util.CommandLineUtils;
import com.sliva.btc.scanner.util.CommandLineUtils.CmdArguments;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.bitcoinj.core.Address;

/**
 *
 * @author whost
 */
public class RunAddress {

    private static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions().add(DBConnectionSupplier.class);

    private final DbQueryAddressOne queryAddress;

    /**
     * @param args the command line arguments
     * @throws java.lang.Exception
     */
    public static void main(String[] args) throws Exception {
        CmdArguments cmd = buildCmdArguments(args, Main.Command.address.name(), "Convert addresses between string hash and DB ID formats", "address1 [ address2 ...]", CMD_OPTS);
        new RunAddress().runProcess(args);
    }

    @SuppressWarnings("UseSpecificCatch")
    public RunAddress() {
        DBConnectionSupplier c = new DBConnectionSupplier();
        queryAddress = new DbQueryAddressCombo(c);

    }

    private void runProcess(String[] args) {
        Stream.of(args).filter(s -> !s.startsWith("-")).forEach(this::processAddress);
    }

    @SuppressWarnings("UseSpecificCatch")
    private void processAddress(String a) {
        if (a.length() <= 10 && StringUtils.isNumeric(a)) {
            try {
                BtcAddress btcAddress = queryAddress.findByAddressId(Integer.parseInt(a)).orElseThrow(() -> new IllegalArgumentException("Address not found: " + a));
                System.out.println("Address for DB ID " + a + " is: " + btcAddress.getBjAddress()
                        + ", type: " + btcAddress.getType() + ", hash: " + Hex.encodeHexString(btcAddress.getAddress(), true));
            } catch (Exception e) {
                System.out.println("Error processing " + a + ": " + e.getMessage());
            }
        } else if (a.matches("[0-9a-fA-F]*") && a.length() == 40 || a.length() == 64) {
            if (queryAddress != null) {
                try {
                    Optional<BtcAddress> btcAddressOpt = queryAddress.findByAddress(Hex.decodeHex(a));
                    if (btcAddressOpt.isPresent()) {
                        BtcAddress btcAddress = btcAddressOpt.get();
                        System.out.println("Address for hash " + a + " is: " + btcAddress.getBjAddress()
                                + ", type: " + btcAddress.getType() + ", DB ID: " + btcAddress.getAddressId());
                        return;
                    }
                } catch (Exception e) {
                    //
                }
            }
            Stream.of(SrcAddressType.values()).filter(SrcAddressType::isReal).forEach(type -> {
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
}
