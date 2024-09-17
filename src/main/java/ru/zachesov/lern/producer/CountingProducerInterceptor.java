package ru.zachesov.lern.producer;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CountingProducerInterceptor implements ProducerInterceptor<Object, Object> {

  ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
  static AtomicLong numSent = new AtomicLong(0);
  static AtomicLong numAcked = new AtomicLong(0);

  public void configure(Map<String, ?> map) {
    long windowSize = Long.parseLong((String) map.get("counting.interceptor.window.size.ms"));
    executorService.scheduleAtFixedRate(
        CountingProducerInterceptor::run, windowSize, windowSize, TimeUnit.MILLISECONDS);
  }

  public ProducerRecord<Object, Object> onSend(ProducerRecord<Object, Object> producerRecord) {
    numSent.incrementAndGet();
    return producerRecord;
  }

  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
    numAcked.incrementAndGet();
  }

  public void close() {
    executorService.shutdownNow();
  }

  public static void run() {
    System.out.println(numSent.getAndSet(0));
    System.out.println(numAcked.getAndSet(0));
  }
}
