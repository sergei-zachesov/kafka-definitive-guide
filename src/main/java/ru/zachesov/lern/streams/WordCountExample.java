package ru.zachesov.lern.streams;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class WordCountExample {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // Defining the logic of the application
    StreamsBuilder builder = new StreamsBuilder();
    // We read from a kafka topic named wordcount-input
    KStream<String, String> source = builder.stream("wordcount-input");
    final Pattern pattern = Pattern.compile("\\W+");

    KStream<String, String> counts =
        source
            .flatMapValues(value -> List.of(pattern.split(value.toLowerCase())))
            .map((key, value) -> new KeyValue<>(value, value))
            .filter((key, value) -> (!value.equals("the")))
            .groupByKey()
            .count()
            .mapValues(value -> Long.toString(value))
            .toStream();

    counts.to("wordcount-output"); // We write the results to a kafka topic named wordcount-output

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();
    // usually the stream application would be running forever,
    // in this example we just let it run for some time and stop since the input data is finite.
    Thread.sleep(5000L);
    streams.close();
  }
}

