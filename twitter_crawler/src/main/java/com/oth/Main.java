package com.oth;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {
    //Max number of images that can be requested in one batch
    private static final int maxNumImages = 100;


    //Initializes the specific AWS services
    private static final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    private static final DynamoDB dynamoDB = new DynamoDB(client);

    private static final String QUEUE_NAME = "HashtagQueue.fifo";
    private static final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
    private static final String queue_url = sqs.getQueueUrl(QUEUE_NAME).getQueueUrl();
    private static final String bearerToken = "";


    public static void main(String[] args) {
        checkQueue();


    }

    /*
     * This method searches the Twitter API for the requested hashtag using the set amount
     * */
    private static void search(String searchString, int amount) throws IOException, URISyntaxException {
        String searchResponse = null;
        int numTweets = 0;
        Table table = dynamoDB.getTable("twitterimageDatabase");

        //checks DynamoDB whether any pictures are still saved
        int persistedLength = checkSavedPictures(searchString, table);
        System.out.println("found " + persistedLength + " already saved pictures!");

        //only download new pictures if the saved ones are not enough to satisfy request
        int numNewTweets = amount - persistedLength;

        //Twitter only allows up to 100 tweets per batch, if more are requested you need multiple batches
        while (numTweets < numNewTweets) {

            HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom()
                    .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();

            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/recent");
            ArrayList<NameValuePair> queryParameters;

            //Query Parameters for the Twitter API, defines the search term, restrictions and expansions (for more information)
            queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", "#" + searchString + " has:images -is:retweet -is:quote"));
            queryParameters.add(new BasicNameValuePair("expansions", "attachments.media_keys"));
            queryParameters.add(new BasicNameValuePair("media.fields", "url"));
            //If more than 100 are requested, only request 100 and then wait for the next batch, otherwise search for the given amount
            if (numNewTweets - numTweets > 100) {
                queryParameters.add(new BasicNameValuePair("max_results", Integer.toString(maxNumImages)));
            } else {
                queryParameters.add(new BasicNameValuePair("max_results", Integer.toString(numNewTweets - numTweets)));
            }

            //Twitter batches are identified by tokens
            if (numTweets != 0) {
                queryParameters.add(new BasicNameValuePair("next_token", getNextToken(searchResponse)));

            }
            uriBuilder.addParameters(queryParameters);

            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpGet.setHeader("Content-Type", "application/json");

            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();

            if (null != entity) {
                searchResponse = EntityUtils.toString(entity, "UTF-8");
                numTweets += getPicture(searchResponse, searchString, numNewTweets - numTweets);

            }
        }
    }

    /*
     * This method searches the dynamoDB for entries with a given hashtag and returns the number of entries
     * */
    private static int checkSavedPictures(String searchString, Table table) {
        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("Hashtag = :v_id")
                .withValueMap(new ValueMap()
                        .withString(":v_id", searchString));

        ItemCollection<QueryOutcome> items = table.query(spec);

        return items.getAccumulatedItemCount();
    }

    /*
     * This method parses the API response.
     * Parses Picture URL and the Media Key(unique identifier) and calls SQSAccess to send the parsed values.
     * */
    private static int getPicture(String line, String hashtag, int numNewTweets) throws MalformedURLException {
        String regexURL = "https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        String regexMediaKey = "[0-9]*_[0-9]*\",\"type\":\"photo\"";
        String regexSplit = "\",\"type\":\"photo\",\"url\":\"";
        String entireLine = "[0-9]*_[0-9]*\",\"type\":\"photo\",\"url\":\"https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern pattern = Pattern.compile(entireLine, Pattern.CASE_INSENSITIVE);

        Matcher urlMatcher = pattern.matcher(line);
        ArrayList<String> lineList = new ArrayList<String>();
        String[] temp;

        while (urlMatcher.find() && lineList.size() < maxNumImages && lineList.size() < numNewTweets) { //ensures that the total amount of pictures doesn't exceed the maximum
            lineList.add(line.substring(urlMatcher.start(0),
                    urlMatcher.end(0)));
        }

        for (String currLine : lineList) {
            temp = currLine.split(regexSplit);
            System.out.println(Arrays.toString(temp));

            SQSAccess.insertQueue(temp[1], temp[0], hashtag);
        }
        return lineList.size();


    }

    /*
     * This method parses the API response.
     * Parses the next_token parameter for the next batch.
     * */
    private static String getNextToken(String line) {
        String regex = "\"next_token\":\"*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher tokenMatcher = pattern.matcher(line);
        String next_token, result = "";
        if (tokenMatcher.find()) {
            next_token = line.substring(tokenMatcher.start(0), tokenMatcher.end(0));
            result = next_token.substring(14);
        }
        return result;
    }

    /*
     * This method waits until a new message is sent to the hashtag queue.
     * The website sends a message once a user request a certain hashtag, this message is consumed here.
     * Messages have the following structure:
     * Body: hashtag, e.g "dogs"
     * Attributes: amount, e.g 20
     * */
    private static void checkQueue() {
        while (true) {
            try {
                ReceiveMessageRequest messageRequest = new ReceiveMessageRequest().withMessageAttributeNames("amount").withQueueUrl(queue_url).withWaitTimeSeconds(20);
                List<Message> messages = sqs.receiveMessage(messageRequest).getMessages();
                Map<String, MessageAttributeValue> attributes;
                String hashtag, amount;
                int intAmount;


                for (Message m : messages) {
                    sqs.deleteMessage(queue_url, m.getReceiptHandle());
                    System.out.println("Message ID " + m.getMessageId() + " Attribute: " + m.getMessageAttributes().toString() + " Body: " + m.getBody());
                    hashtag = m.getBody();
                    attributes = m.getMessageAttributes();
                    amount = attributes.get("amount").getStringValue();
                    intAmount = Integer.parseInt(amount);
                    search(hashtag, intAmount);

                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
