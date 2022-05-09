/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.nus.cool.queryserver;

import com.nus.cool.queryserver.singleton.*;
import org.apache.zookeeper.KeeperException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;

@SpringBootApplication
public class Application {

    public enum Role { STANDALONE, WORKER, BROKER }

    /**
     * Run server
     * @param args args[0]: data source path
     *             args[1]: port
     *             args[2]: role, WORKER or BROKER,
     * @throws InterruptedException  ZKConnection
     * @throws KeeperException ZKConnection
     * @throws IOException HDFSConnection
     * @throws URISyntaxException HDFSConnection
     */
    public static void main(String[] args) throws InterruptedException, KeeperException, IOException, URISyntaxException {

//        String rawDataSource = "datasetSource/";
//        String rawPort = "9009";
//        String rawRole = "STANDALONE";

        String rawDataSource = args[0];
        String rawPort = args[1];
        String rawRole = args[2];

        Role role = Role.valueOf(rawRole);
        ModelPathCfg.dataSourcePath = rawDataSource;

        System.out.printf("Query server version0.0.1, input DataSource=%s, port=%s, role=%s\n", rawDataSource, rawPort, rawRole);

        ZKConnection zk;

        // get local address.
        String host = InetAddress.getLocalHost().getHostAddress();

        switch (role) {
            case STANDALONE:
                break;
            case BROKER:
                // 1. connect to HDFS and zookeeper
                HDFSConnection.getInstance();
                zk = ZKConnection.getInstance();
                // 2. add address to broker
                zk.createBroker(host + ":"+rawPort);
                // 3. run zookeeper watcher.
                WorkerWatcher workerWatcher = new WorkerWatcher(zk);
                // 4. init singleton
                TaskQueue.getInstance();
                WorkerIndex.getInstance();
                QueryIndex.getInstance();

                Thread consumer = new BrokerConsumerThread();
                consumer.start();
                break;
            case WORKER:
                // 1. connect to HDFS and zookeeper
                HDFSConnection.getInstance();
                zk = ZKConnection.getInstance();
                // 2. register worker to zookeeper
                zk.addWorker(host + ":"+rawPort);
                break;

            default:
                throw new IllegalArgumentException();
        }

        // Start service
        SpringApplication app = new SpringApplication(Application.class);
        app.setDefaultProperties(Collections.singletonMap("server.port", rawPort));
        app.run();
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
        return args -> {

            System.out.println("Let's inspect the beans provided by Spring Boot:");

            String[] beanNames = ctx.getBeanDefinitionNames();
            Arrays.sort(beanNames);
            for (String beanName : beanNames) {
                System.out.println(beanName);
            }
        };
    }

}