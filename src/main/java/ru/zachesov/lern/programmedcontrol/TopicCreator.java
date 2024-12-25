package ru.zachesov.lern.programmedcontrol;

import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

public class TopicCreator {

  AdminClient admin;

  public TopicCreator(AdminClient admin) {
    this.admin = admin;
  }

  // Example of a method that will create a topic if its name starts with "test"
  public void maybeCreateTopic(String topicName) throws ExecutionException, InterruptedException {
    Collection<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic(topicName, 1, (short) 1));
    if (topicName.toLowerCase().startsWith("test")) {
      admin.createTopics(topics);
      // alter configs just to demonstrate a point
      ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
      ConfigEntry compaction =
          new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
      Collection<AlterConfigOp> configOp = new ArrayList<>();
      configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
      Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
      alterConf.put(configResource, configOp);
      admin.incrementalAlterConfigs(alterConf).all().get();
    }
  }
}
