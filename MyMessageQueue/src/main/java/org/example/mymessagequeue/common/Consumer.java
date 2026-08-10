package org.example.mymessagequeue.common;

import org.example.mymessagequeue.mqserver.coreentity.BasicProperties;

import java.io.IOException;

@FunctionalInterface
public interface Consumer {
    void deliverMessage(String conseumerTag, BasicProperties basicProperties,byte[] bytes) throws IOException;
}
