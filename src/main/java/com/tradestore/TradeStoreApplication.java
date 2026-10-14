package com.tradestore;

import java.util.TimeZone;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class TradeStoreApplication {

    public static void main(String[] args) {
        // The store runs in UTC. The PostgreSQL driver sends the JVM default zone to the server on connect, so the
        // default is fixed here instead of depending on the host (Windows, for example, reports "Asia/Calcutta",
        // which PostgreSQL images without legacy tzdata reject). Tests set it with -Duser.timezone=UTC in pom.xml.
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        SpringApplication.run(TradeStoreApplication.class, args);
    }
}
