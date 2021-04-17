package com.oth;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Main {

    private static final int maxImages = 10;
    private static final int numImages = 10;


    public static void main(String[] args) throws IOException, URISyntaxException {
        String bearerToken = "";
        //Replace the search term with a term of your choice
        search("nba", bearerToken);
    }

    /*
     * This method calls the recent search endpoint with a the search term passed to it as a query parameter
     * */
    private static void search(String searchString, String bearerToken) throws IOException, URISyntaxException {
        String searchResponse = null;
        int numTweets = 0;

        while (numTweets < maxImages) {

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
            queryParameters.add(new BasicNameValuePair("max_results", Integer.toString(numImages)));

            //Twitter APi returns 1-100 tweets at a time
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
                numTweets += 10;
                getPicture(searchResponse, searchString);

            }
        }
    }

    private static void getPicture(String line, String hashtag) throws MalformedURLException {
        String regexURL = "https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        String regexMediaKey = "[0-9]*_[0-9]*\",\"type\":\"photo\"";
        String regexSplit = "\",\"type\":\"photo\",\"url\":\"";
        String entireLine = "[0-9]*_[0-9]*\",\"type\":\"photo\",\"url\":\"https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern pattern = Pattern.compile(entireLine, Pattern.CASE_INSENSITIVE);

        Matcher urlMatcher = pattern.matcher(line);
        //ArrayList<String> urlList = new ArrayList<String>();
        //ArrayList<String> mediaKeyList = new ArrayList<String>();
        ArrayList<String> lineList = new ArrayList<String>();
        String[] temp;

        while (urlMatcher.find() && lineList.size() < numImages) { //ensures that the total amount of pictures doesn't exceed the maximum
            lineList.add(line.substring(urlMatcher.start(0),
                    urlMatcher.end(0)));
        }

        for (String currLine : lineList) {
            temp = currLine.split(regexSplit);
            System.out.println(Arrays.toString(temp));
            //mediaKeyList.add(temp[0]);
            //urlList.add(temp[1]);

            SQSAccess.insertQueue(temp[1], temp[0], hashtag);
        }


    }


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

}
