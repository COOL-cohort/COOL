package com.nus.cool.queryserver;


import com.nus.cool.queryserver.singleton.HDFSConnection;
import com.nus.cool.queryserver.singleton.ModelConfig;
import com.nus.cool.queryserver.singleton.QueryIndex;
import com.nus.cool.queryserver.singleton.TaskQueue;
import com.nus.cool.queryserver.singleton.WorkerIndex;
import com.nus.cool.queryserver.singleton.ZKConnection;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.zookeeper.KeeperException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import springfox.documentation.swagger2.annotations.EnableSwagger2;


/**
 * Server main function.
 */
@SpringBootApplication
@EnableSwagger2
public class Application {

  /**
   * Roles.
   */
  public enum Role { STANDALONE, WORKER, BROKER
  }

  /**
   * Run server.
   *
   * @param args empty
   * @throws InterruptedException ZKConnection
   * @throws KeeperException      ZKConnection
   * @throws IOException          HDFSConnection
   * @throws URISyntaxException   HDFSConnection
   */
  public static void main(String[] args) throws
      InterruptedException, KeeperException, IOException, URISyntaxException {

    // read configs
    ModelConfig.getInstance();
    String rawRole = ModelConfig.props.getProperty("run.model");
    String url = "0.0.0.0:" + ModelConfig.props.getProperty("server.port");

    Role role = Role.valueOf(rawRole);

    System.out.printf("Query server version0.0.1, input DataSource=%s, url=%s, role=%s\n",
        ModelConfig.dataSourcePath, url, rawRole);

    ZKConnection zk;

    switch (role) {
      case STANDALONE:
        break;
      case BROKER:
        // 1. connect to HDFS and zookeeper
        HDFSConnection.getInstance();
        zk = ZKConnection.getInstance();
        // 2. add address to broker
        zk.createBroker(url);
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
        zk.addWorker(url);
        break;

      default:
        throw new IllegalArgumentException();
    }

    // Start service
    SpringApplication app = new SpringApplication(Application.class);
    app.run();
  }

  /**
   * Run server.
   *
   * @param ctx empty
   */
  @Bean
  public CommandLineRunner commandLineRunner(ApplicationContext ctx) {
    return args -> {

      System.out.println("Running CoolServer V 0.1 ");
    };
  }

}
