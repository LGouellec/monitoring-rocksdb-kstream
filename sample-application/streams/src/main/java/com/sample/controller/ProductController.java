package com.sample.controller;

import com.sample.helper.DateTimeHelper;
import com.sample.streams.TopologyKafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@RestController
@RequestMapping("products")
public class ProductController {

    @Autowired
    private TopologyKafkaStreams streams;

    private final static DateFormat format = new SimpleDateFormat("dd-MM-yyyy");

    @RequestMapping(value = "/{date}/{id}", method = RequestMethod.GET, produces = "application/json")
    public Float getDayProduct(@PathVariable String date, @PathVariable String id) throws ParseException {

        long timeEpoch = DateTimeHelper.getEpochMidnight(date);

        ReadOnlyWindowStore<String, Float> store = streams
                .getStreams()
                .store(StoreQueryParameters.fromNameAndType(TopologyKafkaStreams.PRODUCT_COMMAND_STORE, QueryableStoreTypes.windowStore()));

        return store.fetch(id, timeEpoch);
    }
}
