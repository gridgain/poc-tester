/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.poc.framework.ssh;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.gridgain.poc.framework.worker.AbstractWorker;
import org.gridgain.poc.framework.worker.task.PocTesterArguments;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static org.gridgain.poc.framework.utils.PocTesterUtils.printer;

public abstract class AbstractCmdExecutor extends AbstractWorker {
    /** */
    private static final Logger LOG = LogManager.getLogger(RemoteSshExecutor.class.getName());

    /** */
    protected String user;

    /** */
    private String keyPath;

    /** */
    private String passPhrase;

    public AbstractCmdExecutor() {
    }

    /**
     *  Default constructor.
     */
    public AbstractCmdExecutor(PocTesterArguments args) {
        this.args = args;
        this.user = args.getUser();
        this.keyPath = args.getKeyPath();
        this.passPhrase = args.getPassPhrase();

        if (this.user == null || this.keyPath == null || !checkKeyPath(this.keyPath))
            tryToSetDefaultIdentity();
    }

    protected Session getSession(String host) throws Exception{
        try {
            java.util.Properties config = new java.util.Properties();

            config.put("StrictHostKeyChecking", "no");
            config.put("PreferredAuthentications", "publickey,keyboard-interactive,password");

            JSch jsch = new JSch();

            jsch.addIdentity(keyPath, passPhrase);

            Session session = jsch.getSession(user, host, 22);

            session.setConfig(config);

            session.connect();

            return session;
        }
        catch (Exception e) {
            printer(String.format("Failed to get connection to the host %s using key %s", host, keyPath));
            printer(e.getMessage());

            throw e;
        }
    }

    protected void tryToSetDefaultIdentity() {
//        String defUser = System.getProperty("user.name");

        List<String> defKeyNames = Arrays.asList("id_rsa", "lab_keys");

        boolean found = false;

        String sshDir = String.format("/home/%s/.ssh", user);

        for (String keyName : defKeyNames) {
            String defKeyPath = Paths.get(sshDir, keyName).toString();

            File key = new File(defKeyPath);

            if (key.exists()) {
                LOG.debug("Found identity key " + defKeyPath + ". Will try to use it for ssh connection.");

                this.keyPath = defKeyPath;

                found = true;

                break;
            }

        }

        if (!found) {
            printer(String.format("Failed to find identity key in %s.", sshDir));

            System.exit(1);
        }

    }

    protected void printHelp(){};


    private boolean checkKeyPath(String keyPath) {
        return new File(keyPath).exists();
    }
}