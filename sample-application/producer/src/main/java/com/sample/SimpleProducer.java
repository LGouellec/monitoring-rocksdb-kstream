package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SimpleProducer {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final Properties properties;
    private final String topicName;
    private final Long messageBackOff;
    private final Long numberMessagePerBatch;
    private final String[] listPersons;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }

    public SimpleProducer() throws ExecutionException, InterruptedException {
        properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX);
        topicName = System.getenv().getOrDefault("TOPIC","sample");

        messageBackOff = Long.valueOf(System.getenv().getOrDefault("MESSAGE_BACKOFF","100"));
        numberMessagePerBatch = Long.valueOf(System.getenv().getOrDefault("NUMBER_MESSAGE_PER_BATCH","100"));
        listPersons = System.getenv().getOrDefault("LIST_PERSONS","admin,test").split(",");

        final Integer numberOfPartitions =  Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS","2"));
        final Short replicationFactor =  Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR","3"));

        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, topicName, numberOfPartitions, replicationFactor);
    }

    private void start() throws InterruptedException {
        logger.info("creating producer with props: {}", properties);
        Random rd = new Random();

        logger.info("Sending data to `{}` topic", topicName);
        try (Producer<String, Long> producer = new KafkaProducer<>(properties)) {

            while (true) {

                for(int i = 0 ; i < numberMessagePerBatch ; ++i){

                    int index = rd.nextInt(listPersons.length);
                    boolean isPositive = rd.nextBoolean();
                    int price = rd.nextInt(1000);
                    price = isPositive ? price : (-1 * price);

                    ProducerRecord<String, Long> record = new ProducerRecord<>(topicName, listPersons[index], (long)price);
                    logger.info("Sending Key = {}, Value = {}", record.key(), record.value());

                    producer.send(record,(recordMetadata, exception) -> sendCallback(record, recordMetadata,exception));
                }

                producer.flush();
                TimeUnit.MILLISECONDS.sleep(messageBackOff);
            }
        }
    }

    private void sendCallback(ProducerRecord<String, Long> record, RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            logger.debug("succeeded sending. offset: {}", recordMetadata.offset());
        } else {
            logger.error("failed sending key: {}" + record.key(), e);
        }
    }

    private Map<String, String> defaultProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        return props;
    }

    private void createTopic(AdminClient adminClient, String topicName, Integer numberOfPartitions, Short replicationFactor) throws InterruptedException, ExecutionException {
        if (!adminClient.listTopics().names().get().contains(topicName)) {
            logger.info("Creating topic {}", topicName);

            final Map<String, String> configs = replicationFactor < 3 ? Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1") : Map.of();

            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            newTopic.configs(configs);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }

}
