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

/*
 * This class waits for messages containing images and then downloads them.
 * */
class ImageDownloader {
    //temporary file needed for download
    private static final String PREFIX = "tempfile";
    private static final String SUFFIX = ".jpg";

    //Inits for AWS services
    private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static final DynamoDB dynamoDB = new DynamoDB(client);

    private static final String QUEUE_NAME = "ImageQueue.fifo";
    private static final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();

    public static void main(String[] args) {
        checkQueue();
    }

    /*
     * This method uploads an image to the S3 Bucket.
     * In order to do this it temporarily saves the image from the twitter url.
     * */
    private static void upload(ArrayList<String> urlList, ArrayList<String> mediaKeyList, String hashtag) {
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

    /*
     * This method inserts an image into the dynamoDB.
     * An entry has the following structure:
     *      Primary partition key:  hashtag, e.g "cats"
     *      Primary sort key:       media key, e.g "3_1386383725894815749"
     *      Time to live:           Time after which the entry gets deleted, currently 24 hours
     * */
    private static void addToDynamo(String hashtag, String media_key) {
        Table table = dynamoDB.getTable("twitterimageDatabase");
        Item item = new Item().withPrimaryKey("hashtag", hashtag).withString("media_key", hashtag + "/" + media_key).withString("delete_time", Long.toString(System.currentTimeMillis() / 1000L) + 86400);
        table.putItem(item);

    }

    /*
     * This method waits until a new message is sent to the image queue by the twitter_crawler.
     * The messages have the following structure:
     * Body: URL, e.g https://pbs.twimg.com/media/Ez1sp0BX0AUpb3b.jpg
     * Attributes:
     *      curr_hashtag, e.g cats
     *      media_key, e.g 3_1386383725894815749
     * */
    private static void checkQueue() {
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
