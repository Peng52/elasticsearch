package com.es.elasticsearch.config;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * @program: elasticsearch
 * @description: Elasticsearch - 配置
 * @author: peng
 * @create: 2019-01-14 23:20
 **/

@Configuration
public class ElasticsearchConfig {
    @Bean
    public TransportClient getClient() throws UnknownHostException {
        //定义一个node节点  通信的地址
        TransportAddress node = new TransportAddress(InetAddress.getByName("localhost"), 9300);
        //定义setting
        Settings settings = Settings.builder().put("cluster.name", "peng").build();
        //
        TransportClient transportClient = new PreBuiltTransportClient(settings);
        transportClient.addTransportAddress(node);

        return transportClient;
    }




}
