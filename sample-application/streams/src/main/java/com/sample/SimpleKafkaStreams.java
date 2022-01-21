package com.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SimpleKafkaStreams {

    public static void main(String[] args)  {
        SpringApplication.run(SimpleKafkaStreams.class, args);
    }
}
