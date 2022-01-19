package com.sample;

import com.sample.avro.Command;
import com.sample.avro.Product;
import com.sample.avro.ProductCommand;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
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
    private final String catalogProductsPath;
    private List<Product> products;
    private List<String> users;

    public static void main(String[] args) throws Exception {
        SimpleProducer simpleProducer = new SimpleProducer();
        simpleProducer.start();
    }

    public SimpleProducer() throws Exception {
        properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX);
        topicName = System.getenv().getOrDefault("TOPIC","sample");

        messageBackOff = Long.valueOf(System.getenv().getOrDefault("MESSAGE_BACKOFF","100"));
        numberMessagePerBatch = Long.valueOf(System.getenv().getOrDefault("NUMBER_MESSAGE_PER_BATCH","10"));
        catalogProductsPath = System.getenv().getOrDefault("CATALOG_PRODUCTS_PATH","");

        final Integer numberOfPartitions =  Integer.valueOf(System.getenv().getOrDefault("NUMBER_OF_PARTITIONS","2"));
        final Short replicationFactor =  Short.valueOf(System.getenv().getOrDefault("REPLICATION_FACTOR","3"));

        AdminClient adminClient = KafkaAdminClient.create(properties);
        createTopic(adminClient, topicName, numberOfPartitions, replicationFactor);

        initializeCatalogProducts();
        initializeUsers();
    }

    private void initializeUsers() throws URISyntaxException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream("users");
        users = new ArrayList<>();
        BufferedReader br = null;
        try
        {
            br = new BufferedReader(new InputStreamReader(stream));
            String line;
            while((line=br.readLine())!=null)
                users.add(line);
        }catch(FileNotFoundException fnfe)
        {
            System.out.println("The specified file not found" + fnfe);
        }
        catch(IOException ioe)
        {
            System.out.println("I/O Exception: " + ioe);
        }
        finally
        {
            try{
                if(br!=null)
                    br.close();
                if(stream != null)
                    stream.close();
            }catch(IOException ioe)
            {
                System.out.println("Error in file/buffered reader close(): " + ioe);
            }
        }
    }

    private void initializeCatalogProducts() throws Exception {
        products = new ArrayList<>();
        ProductParser productParser = new ProductParser();

        if(!catalogProductsPath.isEmpty()){
            FileReader fr = null;
            BufferedReader br = null;
            try
            {
                fr = new FileReader(catalogProductsPath);
                br = new BufferedReader(fr);
                String line;
                while((line=br.readLine())!=null)
                    products.add(productParser.parse(line));
            }catch(FileNotFoundException fnfe)
            {
                System.out.println("The specified file not found" + fnfe);
            }
            catch(IOException ioe)
            {
                System.out.println("I/O Exception: " + ioe);
            }
            finally
            {
                try{
                    if(fr != null && br!=null)
                    {
                        fr.close();
                        br.close();
                    }
                }catch(IOException ioe)
                {
                    System.out.println("Error in file/buffered reader close(): " + ioe);
                }
            }
        }
    }

    private void start() throws InterruptedException {
        if(products.size() == 0)
            return;

        logger.info("creating producer with props: {}", properties);
        logger.info("Sending data to `{}` topic", topicName);
        try (Producer<String, Command> producer = new KafkaProducer<>(properties)) {

            while (true) {
                pushCommands(producer);
                producer.flush();
                TimeUnit.MILLISECONDS.sleep(messageBackOff);
            }
        }
    }

    private void pushCommands(Producer<String, Command> producer) {

        Random rd = new Random();
        for(int i = 0 ; i < numberMessagePerBatch ; ++i){
            int numberProducts = rd.nextInt(10);
            List<Product> _products = new ArrayList<>();
            List<ProductCommand> _items = new ArrayList<>();
            float total = 0F;
            String user = users.get(rd.nextInt(users.size()));

            numberProducts = numberProducts > 1 ? numberProducts : 1;
            for(int j = 0 ; j < numberProducts ; ++j) {
                int index = rd.nextInt(products.size());
                int qty = rd.nextInt(9) + 1;
                Product p = products.get(index);
                _products.add(p);
                _items.add(new ProductCommand(p.getProductId(), qty));
                total += (p.getPrice() * qty);
            }

            Command command = new Command();
            command.setCommandId(UUID.randomUUID().toString());
            command.setCommandDate(Instant.now());
            command.setProducts(_products);
            command.setItems(_items);
            command.setTotal(total);
            command.setUser(user);

            ProducerRecord<String, Command> record = new ProducerRecord<>(topicName, command.getCommandId(), command);
            logger.info("Sending command.id = {}, command.date = {}, command.user = {}", record.key(), record.value().getCommandDate(), record.value().getUser());
            producer.send(record,(recordMetadata, exception) -> sendCallback(record, recordMetadata,exception));
        }
    }

    private void sendCallback(ProducerRecord<String, Command> record, RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            logger.debug("succeeded sending. offset: {}", recordMetadata.offset());
        } else {
            logger.error("failed sending key: {}" + record.key(), e);
        }
    }

    private Map<String, String> defaultProps = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:19093,localhost:19094",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
            AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081",
            AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "true");

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
