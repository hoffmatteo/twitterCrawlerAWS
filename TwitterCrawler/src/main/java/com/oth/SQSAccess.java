package com.oth;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.util.List;


public class SQSAccess {
    private static final String QUEUE_NAME = "TestQueue.fifo";
    private static final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();


    public static void accessQueue() {
        List<Message> messages = sqs.receiveMessage(queue_url).getMessages();

        for (Message m : messages) {
            System.out.println("Message ID " + m.getMessageId() + " Attribute: " + m.getAttributes().toString() + " Body: " + m.getBody());
            sqs.deleteMessage(queue_url, m.getReceiptHandle());
        }
    }


    public static void insertQueue(String url, String media_key, String hashtag) {
        /*if(media_key == null){
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(queue_url)
                    .withMessageBody(hashtag)
                    .withMessageGroupId("hashtag");
            sqs.sendMessage(send_msg_request);

        }
        else {
        */

        System.out.println(hashtag);

        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queue_url)
                .withMessageBody(url)
                .withMessageGroupId("mediakey")
                .addMessageAttributesEntry("curr_hashtag", new MessageAttributeValue().withDataType("String").withStringValue(hashtag))
                .addMessageAttributesEntry("media_key", new MessageAttributeValue().withDataType("String").withStringValue(media_key));

        sqs.sendMessage(send_msg_request);
    }


}

