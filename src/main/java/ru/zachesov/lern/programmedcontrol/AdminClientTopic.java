package ru.zachesov.lern.programmedcontrol;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

public class AdminClientTopic {

  private static final List<String> TOPIC_LIST =
      List.of("topic1", "topic2", "topic3", "topic4", "topic5", "topic6");
  private static final String TOPIC_NAME = "topic1";
  private static final int NUM_PARTITIONS = 1;
  private static final short REP_FACTOR = 1;

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);
    TopicDescription topicDescription;
    // Создание топика
    DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
    try {
      topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();
      System.out.println("Description of demo topic:" + topicDescription);
      if (topicDescription.partitions().size() != NUM_PARTITIONS) {
        System.out.println("Topic has wrong number of partitions. Exiting.");
        System.exit(-1);
      }
    } catch (ExecutionException | InterruptedException e) {
      // exit early for almost all exceptions
      if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
        e.printStackTrace();
        throw e;
      }
      // if we are here, topic doesn't exist
      System.out.println("Topic " + TOPIC_NAME + " does not exist. Going to create it now");
      // Note that number of partitions and replicas is optional. If they are
      // not specified, the defaults configured on the Kafka brokers will be used
      CreateTopicsResult newTopic =
          admin.createTopics(List.of(new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));
      // Check that the topic was created correctly:
      if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
        System.out.println("Topic has wrong number of partitions.");
        System.exit(-1);
      }
    }

    // Удаление топика
    admin.deleteTopics(TOPIC_LIST).all().get();
    // Check that it is gone. Note that due to the async nature of deletes,
    // it is possible that at this point the topic still exists
    try {
      topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();
      System.out.println("Topic " + TOPIC_NAME + " is still around");
    } catch (ExecutionException e) {
      System.out.println("Topic " + TOPIC_NAME + " is gone");
    }
  }
}
