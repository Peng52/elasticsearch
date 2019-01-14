package com.es.elasticsearch;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.ws.RequestWrapper;
import java.util.Map;

@SpringBootApplication
@RestController
public class ElasticsearchApplication {

    @Autowired
    private TransportClient client;

    @RequestMapping(value = "/get/{index}/{type}/{id}",method = RequestMethod.GET)
    public ResponseEntity get(@PathVariable("index") String index, @PathVariable("type")String type,@PathVariable("id") String id){
        GetResponse result = client.prepareGet(index, type, id).get();
        Map<String, Object> source = result.getSource();
        return new ResponseEntity(source, HttpStatus.OK);
    }


    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }

}

