package com.es.elasticsearch;


import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@SpringBootApplication
@RestController
public class ElasticsearchApplication {

    @Autowired
    private TransportClient client;
    //指定ID查询
    @RequestMapping(value = "/get/{index}/{type}/{id}",method = RequestMethod.GET)
    public ResponseEntity get(@PathVariable("index") String index, @PathVariable("type")String type,@PathVariable("id") String id){
        //指定索引、类型、ID 查询
        GetResponse result = client.prepareGet(index, type, id).get();
        if(!result.isExists()){
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        Map<String, Object> source = result.getSource();
        return new ResponseEntity(source, HttpStatus.OK);
    }

    //添加一个文档
    @RequestMapping(value = "/add/bank/user",method = RequestMethod.POST)
    public ResponseEntity insert(
            @RequestParam("account_number") Integer accountNumber,
            @RequestParam("balance") Integer balance,
            @RequestParam("firstname") String firstname,
            @RequestParam("lastname") String lastname,
            @RequestParam("age") Integer age,
            @RequestParam("gender") String gender,
            @RequestParam("address") String address,
            @RequestParam("employer") String employer,
            @RequestParam("email") String email,
            @RequestParam("city") String city,
            @RequestParam("state") String state
    ){
        //构建一个Json数据格式
        try {
            XContentBuilder json = XContentFactory.jsonBuilder().startObject()
                    .field("account_name", accountNumber)
                    .field("balance", balance)
                    .field("firstname", firstname)
                    .field("lastname", lastname)
                    .field("age", age)
                    .field("gender", gender)
                    .field("address", address)
                    .field("employer", employer)
                    .field("email", email)
                    .field("city", city)
                    .field("state", state)
                    .endObject();
            IndexResponse result = client.prepareIndex("bank", "user").setSource(json).get();
            return new ResponseEntity(result.getId(),HttpStatus.OK);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }
    //删除接口的开发
    @RequestMapping(value = "/delete/bank/{id}",method = RequestMethod.DELETE)
    public ResponseEntity deleted(@PathVariable("id") String  id){
        DeleteResponse deleteResponse = client.prepareDelete("bank", "user", id).get();

        return new ResponseEntity(deleteResponse.getResult().toString(),HttpStatus.OK);

    }

    //更新接口开发

    @PutMapping("/update")
    public ResponseEntity update(
            @RequestParam("id") String id,
            @RequestParam(value = "account_number",required = false) String accout_number,
            @RequestParam(value = "firstname",required = false) String firstname
    ){
        //更新
        UpdateRequest updateRequest = new UpdateRequest("bank", "user", id);
        //定义Json结构
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            builder.field("id",id);
            if(StringUtils.isNotBlank(accout_number)){
                builder.field("account_name",accout_number);
            }
            if(StringUtils.isNotBlank(firstname)){
                builder.field("firstname",firstname);
            }
            builder.endObject();
            updateRequest.doc(builder);
        } catch (IOException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        try {
            UpdateResponse result = client.update(updateRequest).get();
            return new ResponseEntity(result.getResult().toString(),HttpStatus.OK);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (ExecutionException e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    //复合查询
    @PostMapping("/query")
    public ResponseEntity query(
            @RequestParam(value = "firstname",required = false) String firstname,
            @RequestParam(value = "gte_age",defaultValue = "0",required = false) Integer gte_age,
            @RequestParam(value = "lte_age") Integer lte_age
    ){
        //bool查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if(StringUtils.isNotBlank(firstname)){
            boolQuery.must(QueryBuilders.matchQuery("firstname",firstname));
        }
        // 范围查询
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
        rangeQuery.from(gte_age);
        if(lte_age!=null && lte_age >0){
            rangeQuery.to(lte_age);
        }
        //把范围查询丢到bool查询类中
        boolQuery.filter(rangeQuery);
        SearchRequestBuilder builder = client.prepareSearch("bank")
                .setTypes("user")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);
        System.out.println(builder);
        SearchResponse searchResponse = builder.get();
        List<Map<String,Object>> list = new ArrayList<>();
        SearchHits hits = searchResponse.getHits();
        // SearchHits 是  SearchHit 的子类
        for(SearchHit hit:hits){
            list.add(hit.getSourceAsMap());
        }
        return new ResponseEntity(list,HttpStatus.OK);

    }










    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }

}

