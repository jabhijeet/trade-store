package com.tradestore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TradeStoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(TradeStoreApplication.class, args);
    }
}
