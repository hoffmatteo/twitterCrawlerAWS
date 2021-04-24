package com.oth.imagedownloader;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class BucketAccess {
    private static final String PREFIX = "tempfile";
    private static final String SUFFIX = ".jpg";
    private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static final DynamoDB dynamoDB = new DynamoDB(client);

    private static final String QUEUE_NAME = "TestQueue.fifo";
    private static final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

    public static void main(String[] args) {
        checkQueue();
    }


    static void upload(ArrayList<String> urlList, ArrayList<String> mediaKeyList, String hashtag) {
        String bucket_name = "twitterimagesoth";

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();


        for (int i = 0; i < mediaKeyList.size(); i++) {
            try (InputStream in = new URL(urlList.get(i)).openStream()) {
                final File tempFile = File.createTempFile(PREFIX, SUFFIX);


                try (FileOutputStream out = new FileOutputStream(tempFile)) {
                    IOUtils.copy(in, out);
                }
                try {
                    PutObjectResult result = s3.putObject(bucket_name, hashtag + "/" + mediaKeyList.get(i), tempFile);
                    //insert hashtag/image2 into dynamodb as value

                    addToDynamo(hashtag, mediaKeyList.get(i));

                    if (!tempFile.delete()) {
                        System.out.println("Couldn't delete file!");
                        return;
                    }
                } catch (Exception e) {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void addToDynamo(String hashtag, String media_key) {
        Table table = dynamoDB.getTable("twitterimageDatabase");
        Item item = new Item().withPrimaryKey("Hashtag", hashtag).withString("S3 Storage", "S3://" + hashtag + "/" + media_key).withString("Delete Time", Long.toString(System.currentTimeMillis() / 1000L) + 3);

        table.putItem(item);

    }

    public static void checkQueue() {
        while (true) {
            try {
                ReceiveMessageRequest messageRequest = new ReceiveMessageRequest().withMessageAttributeNames("media_key", "curr_hashtag").withQueueUrl(queue_url).withWaitTimeSeconds(20);
                List<Message> messages = sqs.receiveMessage(messageRequest).getMessages();
                String url, hashtag = "", media_key;
                Map<String, MessageAttributeValue> attributes;
                ArrayList<String> urlList = new ArrayList<String>();
                ArrayList<String> mediakeyList = new ArrayList<String>();


                for (Message m : messages) {
                    sqs.deleteMessage(queue_url, m.getReceiptHandle());
                    System.out.println("Message ID " + m.getMessageId() + " Attribute: " + m.getMessageAttributes().toString() + " Body: " + m.getBody());
                    url = m.getBody();
                    urlList.add(url);
                    attributes = m.getMessageAttributes();
                    media_key = attributes.get("media_key").getStringValue();
                    mediakeyList.add(media_key);
                    hashtag = attributes.get("curr_hashtag").getStringValue();
                }

                upload(urlList, mediakeyList, hashtag);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
