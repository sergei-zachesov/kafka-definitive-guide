package ru.zachesov.lern.programmedcontrol;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

public class AdminClientMain {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    ListTopicsResult listTopics = admin.listTopics();

    admin.close(Duration.ofSeconds(30));
  }
}
