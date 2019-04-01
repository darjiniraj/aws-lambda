package com;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import org.joda.time.DateTime;

import java.util.UUID;


public class HelloWorld implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    private AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_WEST_2).build();
    private DynamoDB dynamoDB = new DynamoDB(client);

    private static String tableName = "S3Metadata";


    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        S3EventNotification.S3EventNotificationRecord record = event.getRecords().get(0);

        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getKey();

        DateTime eventTime = record.getEventTime();
        String urlDecodedKey = record.getS3().getObject().getUrlDecodedKey();
        String sourceIPAddress = record.getRequestParameters().getSourceIPAddress();
        Long sizeAsLong = record.getS3().getObject().getSizeAsLong();

        System.out.println("Bucket Name is " + bucket);
        System.out.println("File Path is " + key);

        // just for demo
        createDynamoItem(bucket, key,eventTime,urlDecodedKey,sourceIPAddress,sizeAsLong);


        return bucket;
    }

    private void createDynamoItem(String bucketName, String keyName, DateTime eventTime, String decodedUrl, String sourceIP, Long sizeAsLong) {

        Table table = dynamoDB.getTable(tableName);
        try {

            Item item = new Item().withPrimaryKey("ID", UUID.randomUUID().toString()).withString("BucketName", bucketName)
                    .withString("FileName",keyName)
                    .with("EventTime",eventTime.toString())
                    .withString("DecodeUrl",decodedUrl)
                    .withString("SourceIP",sourceIP)
                    .withLong("Size",sizeAsLong);

            table.putItem(item);

        } catch (Exception e) {
            System.err.println("Create item failed.");
            System.err.println(e.getMessage());

        }
    }


}