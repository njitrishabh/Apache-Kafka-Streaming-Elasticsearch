package com.github.njitrishabh.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import kafka.producer.KeyedMessage;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterKafkaProducer {
    Logger logger = LoggerFactory.getLogger(TwitterKafkaProducer.class.getName());
    String topic = "twitter_tweets";

    public TwitterKafkaProducer() {}

    public void run(String consumerKey, String consumerSecret, String token, String secret) throws InterruptedException {
        logger.info("Setup");

        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        BlockingQueue<String> queue = new LinkedBlockingQueue<String>(100);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        //add some track terms.
        endpoint.trackTerms(Lists.newArrayList("bitcoin", "playstation"));

        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        Client client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue)).build();

        //Establish a connection.
        client.connect();

        for(int msgRead =0; msgRead < 100; msgRead++) {
            KeyedMessage<String, String> message = null;
            try {
                message = new KeyedMessage<String, String>(topic, queue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info(String.valueOf(message));
            producer.send(message);
        }
        producer.close();
        client.stop();

        logger.info("End of application");
    }
    public static void main(String[] args) {
        try {
            new TwitterKafkaProducer().run("", "",
                    "", "");
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }
}
