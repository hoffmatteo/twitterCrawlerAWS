package com.oth;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.amazonaws.services.s3.model.Bucket;
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
import com.oth.BucketAccess;



public class Main {

    // To set your enviornment variables in your terminal run the following line:
    // export 'BEARER_TOKEN'='<your_bearer_token>'


    public static void main(String args[]) throws IOException, URISyntaxException {
        String bearerToken = "";
        //Replace the search term with a term of your choice
        search("dogs", bearerToken);
        //SQSAccess.accessQueue();
        //BucketAccess.upload();

    }

    /*
     * This method calls the recent search endpoint with a the search term passed to it as a query parameter
     * */
    private static void search(String searchString, String bearerToken) throws IOException, URISyntaxException {
        String searchResponse = null;
        int numTweets = 0;

        while (numTweets < 10) {

            HttpClient httpClient = HttpClients.custom().setDefaultRequestConfig(RequestConfig.custom()
                    .setCookieSpec(CookieSpecs.STANDARD).build())
                    .build();


            URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/recent");
            System.out.println(uriBuilder.toString());
            ArrayList<NameValuePair> queryParameters;
            queryParameters = new ArrayList<>();
            queryParameters.add(new BasicNameValuePair("query", "#" + searchString + " has:images -is:retweet -is:quote"));
            queryParameters.add(new BasicNameValuePair("expansions", "attachments.media_keys"));
            queryParameters.add(new BasicNameValuePair("media.fields", "url"));
            queryParameters.add(new BasicNameValuePair("max_results", "10"));

            if (numTweets != 0) {
                queryParameters.add(new BasicNameValuePair("next_token", getNextToken(searchResponse)));

            }


            System.out.println(queryParameters);


            uriBuilder.addParameters(queryParameters);


            HttpGet httpGet = new HttpGet(uriBuilder.build());
            httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
            httpGet.setHeader("Content-Type", "application/json");
            System.out.println(httpGet.toString());


            HttpResponse response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();

            if (null != entity) {
                searchResponse = EntityUtils.toString(entity, "UTF-8");
                System.out.println(searchResponse);
                numTweets += 10;
                getPicture(searchResponse, searchString);

            }
        }
    }

    static private void getPicture(String line, String hashtag) throws MalformedURLException {
        String regexURL = "https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        String regexMediaKey = "[0-9]*_[0-9]*\",\"type\":\"photo\"";
        String regexSplit = "\",\"type\":\"photo\",\"url\":\"";
        String entireLine = "[0-9]*_[0-9]*\",\"type\":\"photo\",\"url\":\"https://pbs.twimg.com/media/*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern pattern = Pattern.compile(entireLine, Pattern.CASE_INSENSITIVE);

        Matcher urlMatcher = pattern.matcher(line);
        ArrayList<String> urlList = new ArrayList<String>();
        ArrayList<String> mediaKeyList = new ArrayList<String>();
        ArrayList<String> lineList = new ArrayList<String>();
        String[] temp;

        while (urlMatcher.find()) {
            lineList.add(line.substring(urlMatcher.start(0),
                    urlMatcher.end(0)));
        }

        for(String currLine : lineList) {
            temp = currLine.split(regexSplit);
            System.out.println(Arrays.toString(temp));
            mediaKeyList.add(temp[0]);
            urlList.add(temp[1]);
        }

        System.out.println(urlList);
        System.out.println(mediaKeyList);

        BucketAccess.upload(urlList, mediaKeyList, hashtag);



    }


    public static String getNextToken(String line) {
        String regex = "\"next_token\":\"*[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher tokenMatcher = pattern.matcher(line);
        String next_token, result = "";
        if (tokenMatcher.find()) {
            next_token = line.substring(tokenMatcher.start(0), tokenMatcher.end(0));
            result = next_token.substring(14);
            System.out.println(result);
        }
        return result;


    }

}
