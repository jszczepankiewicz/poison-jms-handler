package com.jszczepankiewicz.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.TextMessage;

import static com.jszczepankiewicz.jms.JmsRedeliveryGuard.REDELIVERY_COUNT_PROPERTY;
import static java.lang.System.currentTimeMillis;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author jszczepankiewicz
 * @since 2017-08-23
 */
public class MockedTextMessageBuilder {

  private String jmsDestination = "someJmsQueueName";
  private Integer redeliveryCount;
  private long jmsTimestamp = currentTimeMillis();
  private String payload = "this is some message payload";
  private String jmsMessageID = "someJmsMessageId";

  MockedTextMessageBuilder jmsDestination(String jmsDestination) {
    this.jmsDestination = jmsDestination;
    return this;
  }

  MockedTextMessageBuilder payload(String payload) {
    this.payload = payload;
    return this;
  }

  MockedTextMessageBuilder jmsMessageID(String jmsMessageID) {
    this.jmsMessageID = jmsMessageID;
    return this;
  }

  MockedTextMessageBuilder redeliveryCount(Integer redeliveryCount) {
    this.redeliveryCount = redeliveryCount;
    return this;
  }

  MockedTextMessageBuilder jmsTimestamp(long jmsTimestamp) {
    this.jmsTimestamp = jmsTimestamp;
    return this;
  }

  TextMessage build() {
    TextMessage test = mock(TextMessage.class);

    try {

      Destination destination = mock(Destination.class);

      when(destination.toString()).thenReturn(jmsDestination);
      when(test.getJMSDestination()).thenReturn(destination);
      when(test.getText()).thenReturn(payload);
      when(test.getJMSMessageID()).thenReturn(jmsMessageID);
      when(test.getJMSTimestamp()).thenReturn(jmsTimestamp);

      if (redeliveryCount == null || redeliveryCount == 0) {
        when(test.getJMSRedelivered()).thenReturn(false);
      } else {
        when(test.getJMSRedelivered()).thenReturn(true);
        when(test.getIntProperty(REDELIVERY_COUNT_PROPERTY)).thenReturn(redeliveryCount);
      }

    } catch (JMSException e) {
      e.printStackTrace();
    }

    return test;
  }

}
