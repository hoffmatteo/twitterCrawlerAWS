package com.oth;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/*
 * This class provides easy access to the Image SQS Queue.
 * */
public class SQSAccess {
    private static final String QUEUE_NAME = "ImageQueue.fifo";
    private static final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();


    /*
     * This method inserts an image into the Queue.
     * The messages have the following structure:
     * Body: URL, e.g https://pbs.twimg.com/media/Ez1sp0BX0AUpb3b.jpg
     * Attributes:
     *      curr_hashtag, e.g cats
     *      media_key, e.g 3_1386383725894815749
     * */
    public static void insertQueue(String url, String media_key, String hashtag) {


        SendMessageRequest send_msg_request = new SendMessageRequest()
                .withQueueUrl(queue_url)
                .withMessageBody(url)
                .withMessageGroupId("mediakey")
                .addMessageAttributesEntry("curr_hashtag", new MessageAttributeValue().withDataType("String").withStringValue(hashtag))
                .addMessageAttributesEntry("media_key", new MessageAttributeValue().withDataType("String").withStringValue(media_key));

        System.out.println(sqs.sendMessage(send_msg_request).getSequenceNumber());
    }


}

