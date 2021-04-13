package com.oth;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;

import java.util.List;


public class SQSAccess {
    private static String QUEUE_NAME = "TestQueue.fifo";

    public static void accessQueue() {
        AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
        String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
        List<Message> messages = sqs.receiveMessage(queue_url).getMessages();

        for(Message m : messages) {
            System.out.println("Message ID " + m.getMessageId() + " Attribute: " + m.getAttributes().toString() + " Body: " + m.getBody());
            sqs.deleteMessage(queue_url, m.getReceiptHandle());
        }


    }
}
