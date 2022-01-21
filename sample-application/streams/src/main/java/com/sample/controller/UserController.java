package com.sample.controller;

import com.sample.streams.TopologyKafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("users")
public class UserController {

    @Autowired
    private TopologyKafkaStreams streams;

    @RequestMapping(value = "/{date}/{id}", method = RequestMethod.GET, produces = "application/json")
    public String getUser(@PathVariable String date, @PathVariable String id) {
        return id;
    }
}
