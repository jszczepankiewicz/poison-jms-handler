package com.jszczepankiewicz.jms;

import org.apache.logging.log4j.Logger;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import static java.lang.System.currentTimeMillis;
import static java.lang.Thread.sleep;
import static org.apache.logging.log4j.LogManager.getLogger;

/**
 * @author jszczepankiewicz
 * @since 2017-08-23
 */
public class JmsRedeliveryGuard {

  private static final Logger LOG = getLogger();

  public static final String REDELIVERY_COUNT_PROPERTY = "JMSXDeliveryCount";

  private final int maxRedeliveryCount;
  private final int maxMessageTTLInSeconds;
  private final int redeliveredMessageOnReceptionDelayInSeconds;

  public JmsRedeliveryGuard(int maxRedeliveryCount, int maxMessageTTLInSeconds, int redeliveredMessageOnReceptionDelayInSeconds) {
    this.maxRedeliveryCount = maxRedeliveryCount;
    this.maxMessageTTLInSeconds = maxMessageTTLInSeconds;
    this.redeliveredMessageOnReceptionDelayInSeconds = redeliveredMessageOnReceptionDelayInSeconds;
  }

  public boolean stopProcessingRedeliveredMessage(TextMessage message) {

    if (checkForNulledMessage(message)) return true;

    try {

      if (message.getJMSRedelivered()) {

        int redeliveryCount = message.getIntProperty(REDELIVERY_COUNT_PROPERTY);

        //  message redelivered, checking redelivery count first
        if (maxRedeliveryCount > 0) {
          if (checkMaxRedeliveryExceeded(message, redeliveryCount)) return true;
        }

        if (maxMessageTTLInSeconds > 0) {
          if (checkTTLExceeded(message)) return true;
        }

        throttleMessageProcessing(message, redeliveryCount);

      } else {
        LOG.debug("Message [{}] detected as NOT redelivered, allowing further processing...", message.getJMSMessageID());
        return false;
      }


    } catch (JMSException e) {
      LOG.error("Unexpected JMSException while checking for redelivery conditions.This should not happen and require " +
              "investigation. Message will not be processed further to avoid potential poison message redelivery.", e);
      return true;
    }

    return false;
  }

  private void throttleMessageProcessing(TextMessage message, int redeliveryCount) throws JMSException {
    if (redeliveredMessageOnReceptionDelayInSeconds > 0) {

      LOG.debug("Message [{}] detected as redelivered {} times. Delaying (throttling) further processing by {} sec ...",
              message.getJMSMessageID(), redeliveryCount, redeliveredMessageOnReceptionDelayInSeconds);

      try {
        sleep(redeliveredMessageOnReceptionDelayInSeconds * 1000);
      } catch (InterruptedException e) {
        LOG.warn("Silently ignoring InterruptedException on throttling redelivered message");
      }


    } else {
      LOG.debug("Message [{}] detected as redelivered {} times. Delaying (throttling) not applied as not configured.",
              message.getJMSMessageID(), redeliveryCount);
    }
  }

  private boolean checkMaxRedeliveryExceeded(TextMessage message, int redeliveryCount) throws JMSException {
    if (redeliveryCount > maxRedeliveryCount) {
      LOG.error("Redelivery count: {} of message [{}] exceeds maxRedeliveryCount: {}, this message will be dropped" +
                      " to prevent poison message redelivery, please investigate it! Message details: {}", redeliveryCount,
              message.getJMSMessageID(), maxRedeliveryCount, message);
      return true;
    } else {
      LOG.debug("Redelivery count: {} of message [{}] does not exceed maxRedeliveryCount: {}, this message will not" +
                      " be rejected because of redelivery count",
              redeliveryCount, message.getJMSMessageID(), maxRedeliveryCount);
    }
    return false;
  }

  private boolean checkTTLExceeded(TextMessage message) throws JMSException {
    long messageLifespanMs = currentTimeMillis() - message.getJMSTimestamp();

    if (messageLifespanMs > (maxMessageTTLInSeconds * 1000)) {
      LOG.error("Message lifespan: {} ms of message [{}] exceeds maxMessageTTLInSeconds: {}, this message will be dropped" +
                      " to prevent poison message redelivery, please investigate it! Message details: {}", messageLifespanMs,
              message.getJMSMessageID(), maxMessageTTLInSeconds, message);
      return true;
    } else {
      LOG.debug("Message lifespan: {} ms of message [{}] does not exceed maxMessageTTLInSeconds: {}, this message will not" +
                      " be rejected because of TTL",
              messageLifespanMs, message.getJMSMessageID(), maxMessageTTLInSeconds);
    }
    return false;
  }

  private boolean checkForNulledMessage(TextMessage message) {
    if (message == null) {
      LOG.error("TextMessage is null! Check your code for errors. Stop processing is being raised to avoid potential" +
              " poison message redelivery.");
      return true;
    }
    return false;
  }

}
