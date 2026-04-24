package com.kvstore.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Configuration
public class AppConfig {

    /**
     * RestTemplate used for inter-node communication.
     * Timeout is set to replicationTimeoutMs so a slow node
     * does not block the coordinator indefinitely.
     */
    @Bean
    public RestTemplate restTemplate(KvStoreProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(props.getReplicationTimeoutMs());
        factory.setReadTimeout(props.getReplicationTimeoutMs());
        RestTemplate restTemplate = new RestTemplate(factory);
        return restTemplate;
    }
}
