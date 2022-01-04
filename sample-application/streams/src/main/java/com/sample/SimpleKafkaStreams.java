package com.sample;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SimpleKafkaStreams {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SimpleKafkaStreams.class);
    private final Properties properties;
    private final String inputTopic;
    private final String outputTopic;

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        SimpleKafkaStreams simpleKafkaStreams = new SimpleKafkaStreams();
        simpleKafkaStreams.start();
    }

    public SimpleKafkaStreams() throws InterruptedException, ExecutionException {
        properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX);
        inputTopic = System.getenv().getOrDefault("INPUT_TOPIC","sample");
        outputTopic = System.getenv().getOrDefault("OUTPUT_TOPIC","output");

        final Integer numberOfPartitions =  Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS","2"));
        final Short replicationFactor =  Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR","3"));

        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, inputTopic, numberOfPartitions, replicationFactor);
        createTopic(adminClient, outputTopic, numberOfPartitions, replicationFactor);

    }

    private void start() throws InterruptedException {
        logger.info("creating streams with props: {}", properties);
        final StreamsBuilder builder = new StreamsBuilder();
        final boolean run = true;

        Materialized<String, Long, KeyValueStore<Bytes, byte[]>> materialized = Materialized.as("account-store");
        materialized.withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());

        KTable<String, Long> count = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.Long()))
                .groupByKey()
                .aggregate(
                        () -> 0L,
                        (k, _new, _old) -> _old + _new,
                        Named.as("aggregate-account-banking-processor"),
                        materialized);

        KStream<String, Long> stream = count
                .toStream();

        stream.to(outputTopic, Produced.with(Serdes.String(),Serdes.Long()));

        // display the 3 large account banking
        stream.process(new ProcessorSupplier<String, Long, Void, Void>() {
            @Override
            public Processor<String, Long, Void, Void> get() {
                return new Processor<String, Long, Void, Void>() {
                    @Override
                    public void process(Record<String, Long> record) {
                        // nothing
                    }

                    @Override
                    public void init(ProcessorContext<Void, Void> context) {
                        Processor.super.init(context);
                        KeyValueStore<String, ValueAndTimestamp<Long>> store = (KeyValueStore<String, ValueAndTimestamp<Long>>)context.getStateStore("account-store");

                        context.schedule(
                                Duration.ofSeconds(30),
                                PunctuationType.WALL_CLOCK_TIME,
                                new Punctuator() {
                                    @Override
                                    public void punctuate(long l) {
                                        long val1 = Long.MIN_VALUE,
                                                val2= Long.MIN_VALUE,
                                                val3= Long.MIN_VALUE;
                                        String key1 = null, key2 = null, key3 = null;

                                        KeyValueIterator<String, ValueAndTimestamp<Long>> iterator = store.all();
                                        while(iterator.hasNext()){
                                            KeyValue<String, ValueAndTimestamp<Long>> kv = iterator.next();
                                            if(kv.value.value() > val1)
                                            {
                                                key3 = key2;
                                                val3 = val2;
                                                key2 = key1;
                                                val2 = val1;
                                                key1 = kv.key;
                                                val1 = kv.value.value();
                                            }
                                            else if(kv.value.value() > val2){
                                                key3 = key2;
                                                val3 = val2;
                                                key2 = kv.key;
                                                val2 = kv.value.value();
                                            }
                                            else if(kv.value.value() > val3){
                                                key3 = kv.key;
                                                val3 = kv.value.value();
                                            }
                                        }
                                        iterator.close();


                                        System.out.println(String.format("Print larges account banking rate for task %s", context.taskId().toString()));
                                        if(key1 != null)
                                            System.out.println(String.format("1st place : %s - %d€", key1, val1));

                                        if(key2 != null)
                                            System.out.println(String.format("2nd place : %s - %d€", key2, val2));

                                        if(key3 != null)
                                            System.out.println(String.format("3rd place : %s - %d€", key3, val3));
                                    }
                                });
                    }
                };
            }
        }, Named.as("display-processor"));

        Topology topology = builder.build();

        topology.connectProcessorAndStateStores("display-processor", "account-store");

        logger.info(topology.describe().toString());

        KafkaStreams streams = new KafkaStreams(topology,properties);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Map<String, String> defaultProps = Map.of(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094",
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID","sample-streams"),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "3"
            );

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
            final NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, replicationFactor);
            try {
                CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                topicsCreationResult.all().get();
            } catch (ExecutionException e) {
                //silent ignore if topic already exists
            }
        }
    }

}
