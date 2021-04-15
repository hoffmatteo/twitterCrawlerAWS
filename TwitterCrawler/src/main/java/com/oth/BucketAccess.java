package com.oth;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.util.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class BucketAccess {
    private static final String PREFIX = "stream2file";
    private static final String SUFFIX = ".jpg";
    private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static DynamoDB dynamoDB = new DynamoDB(client);



    static void upload(ArrayList<String> urlList, ArrayList<String> mediaKeyList, String hashtag) {
        String bucket_name = "twitterimagesoth";

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();



        for(int i = 0; i < mediaKeyList.size(); i++) {
            try (InputStream in = new URL(urlList.get(i)).openStream()) {
                final File tempFile = File.createTempFile(PREFIX, SUFFIX);


                try (FileOutputStream out = new FileOutputStream(tempFile)) {
                    IOUtils.copy(in, out);
                }
                try {
                    PutObjectResult result = s3.putObject(bucket_name, hashtag + "/" + mediaKeyList.get(i), tempFile);
                    //insert hashtag/image2 into dynamodb as value

                    addToDynamo(hashtag, mediaKeyList.get(i));

                    if(!tempFile.delete()) {
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
        Item item = new Item().withPrimaryKey("Hashtag", hashtag).withString("S3 Storage", "S3://" + hashtag + "/" + media_key);

        table.putItem(item);

    }
}
