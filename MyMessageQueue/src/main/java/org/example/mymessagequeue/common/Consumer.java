package org.example.mymessagequeue.common;

import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;

@FunctionalInterface
public interface Consumer {
    void deliverMessage(String conseumerTag, BasicProperties basicProperties,byte[] bytes);
}
