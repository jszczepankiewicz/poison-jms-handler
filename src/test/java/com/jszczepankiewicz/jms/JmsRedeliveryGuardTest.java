package com.jszczepankiewicz.jms;


import org.junit.Test;

import javax.jms.TextMessage;

import static java.lang.System.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author jszczepankiewicz
 * @since 2017-08-23
 */
public class JmsRedeliveryGuardTest {

  private static final int MAX_REDELIVERY_10_TIMES = 10;
  private static final int MAX_TTL_10_SECONDS = 10;
  private static final int REDELIVERED_MESSAGE_THROTTLE_DELAY_ONE_SECOND = 1;

  private JmsRedeliveryGuard defaultJmsRedeliveryGuard(){
    return new JmsRedeliveryGuard(MAX_REDELIVERY_10_TIMES, MAX_TTL_10_SECONDS, REDELIVERED_MESSAGE_THROTTLE_DELAY_ONE_SECOND);
  }

  @Test
  public void stopProcessingMessageIfNullPassed() {

    //  given
    JmsRedeliveryGuard guard = defaultJmsRedeliveryGuard();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(null);

    //  then
    //  if there was some nulled message passed we should stop processing message but do NOT throw any exception
    //  this is to avoid poison message being redelivered to broker, we assume underlying object is putting relevant
    //  information into logger (but do not check in this test as it would require to inject logger
    assertThat(stopProcessing).isTrue();
  }

  @Test
  public void processMessagesWithRedeliveryCountEqualToMax(){

    //  given
    JmsRedeliveryGuard guard = defaultJmsRedeliveryGuard();
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(MAX_REDELIVERY_10_TIMES).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isFalse();
  }

  @Test
  public void stopProcessingMessageOnRedeliveryExceeded() {

    //  given
    JmsRedeliveryGuard guard = defaultJmsRedeliveryGuard();
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(MAX_REDELIVERY_10_TIMES + 1).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isTrue();

  }

  @Test
  public void processIfRedeliveryCountSetTo0() {

    //  given
    JmsRedeliveryGuard guard = new JmsRedeliveryGuard(0, MAX_TTL_10_SECONDS, REDELIVERED_MESSAGE_THROTTLE_DELAY_ONE_SECOND);
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(100).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isFalse();

  }

  @Test
  public void processIfTTLSetTo0() {

    //  given
    long ancientTimestamp = 1;
    JmsRedeliveryGuard guard = new JmsRedeliveryGuard(MAX_REDELIVERY_10_TIMES, 0, REDELIVERED_MESSAGE_THROTTLE_DELAY_ONE_SECOND);
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(2).jmsTimestamp(ancientTimestamp).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isFalse();
  }



  @Test
  public void processIfRedeliveryCountSetTo0AndTtlTo0() {

    //  given
    long ancientTimestamp = 1;
    JmsRedeliveryGuard guard = new JmsRedeliveryGuard(0, 0, REDELIVERED_MESSAGE_THROTTLE_DELAY_ONE_SECOND);
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(Integer.MAX_VALUE).jmsTimestamp(ancientTimestamp).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isFalse();
  }

  @Test
  public void processIfBothTtlAndRedeliveryCountNotExceeded() {

    //  given
    JmsRedeliveryGuard guard = defaultJmsRedeliveryGuard();
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(2).jmsTimestamp(currentTimeMillis()).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isFalse();
  }


  //  add more detailed test with mocked currentTimeMilis?
  @Test
  public void stopProcessingMessageOnTtlExceeded() {

    //  given
    JmsRedeliveryGuard guard = defaultJmsRedeliveryGuard();
    TextMessage msg = new MockedTextMessageBuilder().redeliveryCount(2).jmsTimestamp(1).build();

    //  when
    boolean stopProcessing = guard.stopProcessingRedeliveredMessage(msg);

    //  then
    assertThat(stopProcessing).isTrue();
  }

}