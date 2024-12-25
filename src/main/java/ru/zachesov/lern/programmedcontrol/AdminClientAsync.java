package ru.zachesov.lern.programmedcontrol;

import io.vertx.core.Vertx;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.admin.*;

public class AdminClientAsync {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Vertx vertx = Vertx.vertx();

    vertx
        .createHttpServer()
        .requestHandler(
            request -> {
              String topic = request.getParam("topic");
              String timeout = request.getParam("timeout");
              int timeoutMs = NumberUtils.toInt(timeout, 1000);
              DescribeTopicsResult demoTopic =
                  admin.describeTopics(
                      List.of(topic), new DescribeTopicsOptions().timeoutMs(timeoutMs));
              demoTopic
                  .topicNameValues()
                  .get(topic)
                  .whenComplete(
                      (topicDescription, throwable) -> {
                        if (throwable != null) {
                          var chunk =
                              "Error trying to describe topic %s due to %s"
                                  .formatted(topic, throwable.getMessage());
                          request.response().end(chunk);
                        } else {
                          request.response().end(topicDescription.toString());
                        }
                      });
            })
        .listen(8080);
  }
}
