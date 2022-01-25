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

import java.text.ParseException;

@RestController
@RequestMapping("users")
public class UserController {

    @Autowired
    private TopologyKafkaStreams streams;

    @RequestMapping(value = "/{date}/{id}", method = RequestMethod.GET, produces = "application/json")
    public Float getDayUser(@PathVariable String date, @PathVariable String id) throws ParseException {

        long timeEpoch = DateTimeHelper.getEpochMidnight(date);

        ReadOnlyWindowStore<String, Float> store = streams
                .getStreams()
                .store(StoreQueryParameters.fromNameAndType(TopologyKafkaStreams.USER_COMMAND_STORE, QueryableStoreTypes.windowStore()));

        return store.fetch(id, timeEpoch);
    }
}
