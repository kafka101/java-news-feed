package io.github.kafka101.newsfeed.producer;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EmbeddedKafkaTest {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedKafkaTest.class);

    protected static String zkConnect;
    protected static String kafkaConnect;

    private static EmbeddedZookeeper zkServer;
    private static ZkClient zkClient;
    private static KafkaServer kafkaServer;

    private static List<KafkaServer> servers = new ArrayList();

    public static void setUp() {
        // setup Zookeeper
        zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        int brokerId = 0;
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
        kafkaConnect = kafkaServer.config().hostName() + ":" + kafkaServer.config().port();
    }

    public static void tearDown() {
        zkClient.close();
        kafkaServer.shutdown();
        zkServer.shutdown();
        servers.clear();
    }

    protected void createTopic(String topic) {
        Properties properties = new Properties();
        properties.put("cleanup.policy", "compact");
        AdminUtils.createTopic(zkClient, topic, 1, 1, properties);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0,
                10000);
    }
}
