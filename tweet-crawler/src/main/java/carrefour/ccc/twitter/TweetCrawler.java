package carrefour.ccc.twitter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

import java.util.Arrays;

/**
 * Created by mehdi on 24/09/16.
 */
public class TweetCrawler {

    static final String PATH = "/tmp/tweets";


    public static void main(String[] args) {


        SparkConf sparkConf = new SparkConf().setAppName("Tweet Crawler").setMaster("local[2]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        //String[] filters = {"fraise", "mandarine", "cerise", "legume", "fruit", "raisin"};
        String[] filters = {};
        JavaReceiverInputDStream<Status> stream = selectTweetsByFilter(sc, filters);


        //selectTweetsByLang(stream, "fr").print();
        selectTweetsByLang(selectTweetsByLocation(stream, "paris"), "fr").print();


        sc.start();
        sc.awaitTermination();


    }

    static public JavaReceiverInputDStream<Status> selectTweetsByFilter(JavaStreamingContext sc, String[] filters) {
        Configuration twitterConf = ConfigurationContext.getInstance();
        Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(sc, twitterAuth, filters);
        return stream;
    }


    static public JavaDStream<Status> selectTweetsByLang(JavaDStream<Status> stream, final String language) {
        return stream.filter(new Function<Status, Boolean>() {
            public Boolean call(Status status) throws Exception {
                if (status.getLang().equals(language)) {
                    return true;
                } else return false;
            }
        });
    }


    static public JavaDStream<Status> selectTweetsByLocation(JavaReceiverInputDStream<Status> stream, final String city) {
        return stream.filter(new Function<Status, Boolean>() {
            public Boolean call(Status status) throws Exception {
                return status.toString().toLowerCase().contains(city.toLowerCase());
            }
        });
    }


}
