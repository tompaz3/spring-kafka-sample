package com.tp.samples.springkafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@SpringBootTest(properties = {"spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"})
@ExtendWith(SpringExtension.class)
@DirtiesContext
@EmbeddedKafka(partitions = 4, topics = {NewsConstants.NEWS_TOPIC})
public class SpringKafkaApplicationTest {

  @Autowired
  private MessageProducer messageProducer;
  @Autowired
  private MessageListener messageListener;

  @Test
  void testSendReceive() throws Exception {
    messageProducer.sendMessage("News test message", 2);
    messageListener.latch().await(10, TimeUnit.SECONDS);
    assertThat(messageListener.latch().getCount()).isEqualTo(0);
  }
}
