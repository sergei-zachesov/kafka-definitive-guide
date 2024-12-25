package ru.zachesov.lern.programmedcontrol;

import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;

import java.util.Properties;

public class AdminClientCluster {

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    DescribeClusterResult cluster = admin.describeCluster();
    System.out.println("Connected to cluster " + cluster.clusterId().get());
    System.out.println("The brokers in the cluster are:");
    cluster.nodes().get().forEach(node -> System.out.println(" * " + node));
    System.out.println("The controller is: " + cluster.controller().get());
  }
}
