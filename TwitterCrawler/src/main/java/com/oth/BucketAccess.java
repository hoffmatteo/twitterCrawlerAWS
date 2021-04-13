package com.oth;
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

public class BucketAccess {
    public static final String PREFIX = "stream2file";
    public static final String SUFFIX = ".jpg";


    public static void upload(ArrayList<String> urlList, ArrayList<String> mediaKeyList, String hashtag) {
        String bucket_name = "twitterimagesoth";

        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();


        for(int i = 0; i < mediaKeyList.size(); i++) {
            try (InputStream in = new URL(urlList.get(i)).openStream()) {
                //Files.copy(in, Paths.get("C:/users/mattf/testImages/" + hashtag + "/" + mediaKeyList.get(i) + ".jpg"));
                //Files.copy(in, Paths.get("C:/users/mattf/testImages/" + mediaKeyList.get(i) + ".jpg"));

                //Files.copy(in, Paths.get("/home/ec2-user/images/" + hashtag + "/" + mediaKeyList.get(i) + ".jpg"));
                final File tempFile = File.createTempFile(PREFIX, SUFFIX);

                try (FileOutputStream out = new FileOutputStream(tempFile)) {
                    IOUtils.copy(in, out);
                }
                try {
                    PutObjectResult result = s3.putObject(bucket_name, hashtag + "/" + mediaKeyList.get(i), tempFile);
                    //insert hashtag/image2 into dynamodb as value

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
}
