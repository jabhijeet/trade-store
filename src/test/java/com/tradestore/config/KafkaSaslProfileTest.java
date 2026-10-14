package com.tradestore.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

class KafkaSaslProfileTest {

    @Test
    void kafkaSaslProfile_BindsSaslSslWithScramCredentialsFromEnvironment() throws IOException {
        StandardEnvironment environment = profileEnvironment(Map.of(
                "KAFKA_USERNAME", "trade-store",
                "KAFKA_PASSWORD", "secret",
                "KAFKA_TRUSTSTORE_LOCATION", "file:/etc/kafka/truststore.p12",
                "KAFKA_TRUSTSTORE_PASSWORD", "changeit"));

        KafkaProperties kafka = Binder.get(environment).bind("spring.kafka", KafkaProperties.class).get();

        assertThat(kafka.getSecurity().getProtocol()).isEqualTo("SASL_SSL");
        assertThat(kafka.getProperties())
                .containsEntry("sasl.mechanism", "SCRAM-SHA-512")
                .containsEntry("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule "
                        + "required username=\"trade-store\" password=\"secret\";");
        assertThat(kafka.getSsl().getTrustStoreType()).isEqualTo("PKCS12");
        assertThat(kafka.getSsl().getTrustStorePassword()).isEqualTo("changeit");
    }

    @Test
    void kafkaSaslProfile_WhenCredentialsMissing_StillRequiresSaslSsl() throws IOException {
        StandardEnvironment environment = profileEnvironment(Map.of());

        KafkaProperties kafka = Binder.get(environment).bind("spring.kafka", KafkaProperties.class).get();

        assertThat(kafka.getSecurity().getProtocol()).isEqualTo("SASL_SSL");
        assertThat(kafka.getProperties().get("sasl.jaas.config")).contains("${KAFKA_USERNAME}");
    }

    private static StandardEnvironment profileEnvironment(Map<String, Object> variables) throws IOException {
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(new MapPropertySource("variables", variables));
        new YamlPropertySourceLoader()
                .load("kafka-sasl", new ClassPathResource("application-kafka-sasl.yml"))
                .forEach(environment.getPropertySources()::addLast);
        return environment;
    }
}
