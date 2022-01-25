package com.sample.controller;

import com.sample.avro.Command;
import com.sample.bean.CommandJsonModel;
import com.sample.streams.TopologyKafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("commands")
public class CommandController {

    @Autowired
    private TopologyKafkaStreams streams;

    @RequestMapping(value = "/{id}", method = RequestMethod.GET, produces = "application/json")
    public CommandJsonModel getCommand(@PathVariable String id) {

        ReadOnlyKeyValueStore<String, Command> store = streams
                .getStreams()
                .store(StoreQueryParameters.fromNameAndType(TopologyKafkaStreams.COMMAND_STORE, QueryableStoreTypes.keyValueStore()));

        return CommandJsonModel.to(store.get(id));
    }
}
