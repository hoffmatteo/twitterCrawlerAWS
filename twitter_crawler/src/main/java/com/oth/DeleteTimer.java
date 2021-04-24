package com.oth;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsRequest;
import com.amazonaws.services.cloudwatch.model.DescribeAlarmsResult;
import com.amazonaws.services.cloudwatch.model.MetricAlarm;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

public class DeleteTimer {

    final static String bucket_name = "twitterimagesoth";

    final static AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    final static AmazonCloudWatch cw =
            AmazonCloudWatchClientBuilder.defaultClient();
    private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static final DynamoDB dynamoDB = new DynamoDB(client);


    public static void addHashtag() {

        boolean done = false;
        DescribeAlarmsRequest request = new DescribeAlarmsRequest().withAlarmNames("S3Alarm");


        DescribeAlarmsResult response = cw.describeAlarms(request);


        for (MetricAlarm alarm : response.getMetricAlarms()) {
            System.out.printf("Retrieved alarm %s", alarm.getStateValue());

            if (alarm.getStateValue().equals("INSUFFICIENT_DATA")) {

                ObjectListing objectListing = s3.listObjects(bucket_name);
                while (true) {
                    for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
                        s3.deleteObject(bucket_name, s3ObjectSummary.getKey());

                    }

                    // If the bucket contains many objects, the listObjects() call
                    // might not return all of the objects in the first listing. Check to
                    // see whether the listing was truncated. If so, retrieve the next page of objects
                    // and delete them.
                    if (objectListing.isTruncated()) {
                        objectListing = s3.listNextBatchOfObjects(objectListing);
                    } else {
                        break;
                    }
                }

                //Table table = dynamoDB.getTable("twitterimageDatabase");
                //table.delete();
                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                createTable();


            }
        }

        request.setNextToken(response.getNextToken());


    }


    public static void createTable() {
        String tableName = "twitterimageDatabase";

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("Hashtag", KeyType.HASH), // Partition
                            // key
                            new KeySchemaElement("S3 Storage", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("Hashtag", ScalarAttributeType.S),
                            new AttributeDefinition("S3 Storage", ScalarAttributeType.S)),
                    new ProvisionedThroughput(5L, 5L));
            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.err.println(e.getMessage());
        }

    }

}
