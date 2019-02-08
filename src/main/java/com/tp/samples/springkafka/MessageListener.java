package com.tp.samples.springkafka;

import java.util.concurrent.CountDownLatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Scope("prototype")
public class MessageListener {

  private CountDownLatch latch = new CountDownLatch(1);

  public CountDownLatch latch() {
    return latch;
  }

  @KafkaListener(topics = NewsConstants.NEWS_TOPIC, groupId = NewsConstants.NEWS_GROUP)
  public void listenGroupNews(@Payload String message,
      @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
    log.info("Message received - Topic: {} | Group: {} | Partition: {} | Payload: {}",
        NewsConstants.NEWS_TOPIC, NewsConstants.NEWS_GROUP, partition, message);
    latch.countDown();
  }

}
