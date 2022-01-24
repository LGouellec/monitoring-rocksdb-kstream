package com.sample.controller;

import com.sample.bean.BestProduct;
import com.sample.streams.TopologyKafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

@RestController
@RequestMapping("best-sales")
public class BestProductController {

    @Autowired
    private TopologyKafkaStreams streams;

    private final static DateFormat format = new SimpleDateFormat("dd-MM-yyyy");

    @RequestMapping(value = "/{date}", method = RequestMethod.GET, produces = "application/json")
    public List<BestProduct> getDay(@PathVariable String date) throws ParseException {

        ReadOnlyKeyValueStore<String, List<BestProduct>> store = streams
                .getStreams()
                .store(StoreQueryParameters.fromNameAndType(TopologyKafkaStreams.BEST_SALES_PRODUCT_STORE, QueryableStoreTypes.keyValueStore()));

        return store.get(date);
    }
}
