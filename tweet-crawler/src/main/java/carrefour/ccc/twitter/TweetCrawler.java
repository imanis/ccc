package carrefour.ccc.twitter ;

import java.awt.List;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;

import static org.elasticsearch.common.xcontent.XContentFactory.*;

import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

/**
 * Created by mehdi on 24/09/16.
 */
public class TweetCrawler implements Serializable {

    static final String PATH = "/tmp/tweets";

   static  final TransportClient client = new  TransportClient.Builder().build();
	       
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws UnknownHostException {

    	Logger.getLogger("org").setLevel(Level.OFF);
    	Logger.getLogger("akka").setLevel(Level.OFF);
    	
        SparkConf sparkConf = new SparkConf().setAppName("Tweet Crawler").setMaster("local[2]");
 
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        String[] filters = {"fraise", "mandarine", "cerise", "legume", "fruit", "raisin"};
        JavaReceiverInputDStream<Status> stream = selectTweetsByFilter(sc, filters);
        
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("91.134.229.137"),9300
        		));
		
        JavaDStream<Status> jdsteam  = selectTweetsByLang(stream,"fr");
        jdsteam.
        foreachRDD( new Function<JavaRDD<Status>, Void>() {
        	
			private static final long serialVersionUID = 1L;

			public Void call(JavaRDD<Status> rdd) throws Exception {
				
				
				rdd.foreach(tweet -> {
				
					
					int i=0;
					String term="";
					while(i<filters.length && !tweet.getText().toLowerCase().contains(filters[i].toLowerCase()))
					{
						i++;
					}
					if(i<filters.length)
					{
						term = filters[i];
					}
					 
					XContentBuilder 
					builder
					 = jsonBuilder()
						    .startObject()
						    .field("user", tweet.getUser().getScreenName())
					        .field("post_date",tweet.getCreatedAt())
					        .field("message", tweet.getText())
					        .field("STO_EAN","3020180189392")
					        .field("STORE_DESC", "CITY PARIS -HOPITAL 107")
					       .field("term", term)
						    .endObject();
					
					IndexResponse response = client.prepareIndex("twitter_1", "tweet")
					        .setSource(builder)           
					        .get();
					
					if(tweet.getUser().getScreenName().contains("im_anis")||tweet.getUser().getScreenName().toLowerCase().contains("p3rs0nne".toLowerCase()))
						TweetReply(tweet,term);
					
				
				});
				return null;

}

          });

        sc.start();
        sc.awaitTermination();


    }
    
    static public void TweetReply(Status tweet ,String term)
    {
    	
    	Twitter twittFact = new TwitterFactory().getInstance();
		StatusUpdate st = new StatusUpdate("hello");
		Status reply = null;
		st.inReplyToStatusId(tweet.getId());
		String message =UUID.randomUUID()+" Bonjour @" + tweet.getUser().getScreenName();
	//	message +=  " en parlant de " + term;
//		message +=  " vous trouverez nos promos sur";		
	//	message +=  " http://91.134.229.135:8081/user="+UUID.randomUUID();
		try {
			  reply = twittFact.updateStatus(new StatusUpdate(message).inReplyToStatusId(tweet.getId()));
			  System.out.println("Posted reply " + reply.getId() + " in response to tweet " + reply.getInReplyToStatusId());
		  } catch (TwitterException e) {
		    // TODO Auto-generated catch block
		    e.printStackTrace();
		  }
	
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
