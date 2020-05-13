/*
 * Copyright 2020 Sliva Co.
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
package com.sliva.btc.scanner.rpc;

import com.sliva.btc.scanner.util.CommandLineUtils;
import static com.sliva.btc.scanner.util.CommandLineUtils.buildCmdArguments;
import lombok.SneakyThrows;
import org.apache.commons.cli.ParseException;

/**
 *
 * @author Sliva Co
 */
public class RpcSetup {

    private static final String CONF_FILE = "/etc/rpc.conf";
    private static final CommandLineUtils.CmdOptions CMD_OPTS = new CommandLineUtils.CmdOptions().add(RpcClient.class).add(RpcClientDirect.class);

    @SneakyThrows(ParseException.class)
    public static void init() {
        buildCmdArguments(new String[]{"--rpc-config=" + CONF_FILE}, "", null, null, CMD_OPTS);
    }
}
