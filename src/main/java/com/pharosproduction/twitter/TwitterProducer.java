package com.pharosproduction.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  // Constants

  private static final String CONSUMER_KEY = "";
  private static final String CONSUMER_SECRET = "";
  private static final String TOKEN = "";
  private static final String SECRET = "";

  private static final String TWITTER_CLINET = "kafka-twitter";
  private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
  private static final String TOPIC = "bitcoin_tweets";

  // Variables

  private final Logger mLogger = LoggerFactory.getLogger(TwitterProducer.class.getName());
  private final List<String> mTerms = List.of("bitcoin", "blockchain");

  // Constructor

  private TwitterProducer() {}

  private void run() {
    BlockingQueue<String> msgQueue = new LinkedBlockingDeque<>(1000);
    Client client = twitterConnect(msgQueue);
    KafkaProducer<String, String> producer = createKafkaProducer();

    shutdown(client, producer);

    while (!client.isDone()) {
      String msg = pollQueue(msgQueue, client);
      sendToKafka(msg, producer);
    }

    mLogger.info("We're done");
  }

  private Client twitterConnect(BlockingQueue<String> msgQueue) {
    Client client = createTwitterClient(msgQueue);
    client.connect();

    return client;
  }

  private Client createTwitterClient(BlockingQueue<String> msgQueue) {
    Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
    endpoint.trackTerms(mTerms);

    Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, SECRET);
    ClientBuilder builder = new ClientBuilder()
      .name(TWITTER_CLINET)
      .hosts(hosts)
      .authentication(auth)
      .endpoint(endpoint)
      .processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }

  private String pollQueue(BlockingQueue<String> msgQueue, Client client) {
    try {
      return msgQueue.poll(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      e.printStackTrace();

      client.stop();
    }

    return null;
  }

  private KafkaProducer<String, String> createKafkaProducer() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

    return new KafkaProducer<>(properties);
  }

  private void sendToKafka(String msg, KafkaProducer<String, String> producer) {
    if (msg == null) return;
    mLogger.info(msg);

    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, null, msg);
    producer.send(record, this::logError);
  }

  private void logError(RecordMetadata meta, Exception e) {
    if (e != null) mLogger.error("Unable to create a record: ", e);
  }

  private void shutdown(Client client, KafkaProducer<String, String> producer) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      client.stop();
      producer.close();
    }));
  }
}
