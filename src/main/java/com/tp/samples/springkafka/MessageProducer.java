package com.tp.samples.springkafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

@Slf4j
@Component
public class MessageProducer {

  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  public MessageProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void sendMessage(String message) {
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate
        .send(NewsConstants.NEWS_TOPIC, message);
    logSendResult(future);
  }

  public void sendMessage(String message, int partition) {
    ListenableFuture<SendResult<String, String>> future = kafkaTemplate
        .send(NewsConstants.NEWS_TOPIC, partition, null, message);
    logSendResult(future);
  }

  private void logSendResult(ListenableFuture<SendResult<String, String>> future) {
    future.addCallback(result ->
            log.info("Message sent - Topic: {} | Mesage: {}", NewsConstants.NEWS_TOPIC,
                result != null ? result.getProducerRecord().value() : ""),
        error -> log.error(error.getMessage(), error));
  }


}
