package com.tradestore.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class MongoLiquibaseMigrationTest {

    @ParameterizedTest(name = "{0} -> {1}")
    @CsvSource(delimiter = '|', textBlock = """
            mongodb://localhost:27017                              | mongodb://localhost:27017/tradestore
            mongodb://localhost:27017/                             | mongodb://localhost:27017/tradestore
            mongodb://localhost:27017/tradestore                   | mongodb://localhost:27017/tradestore
            mongodb://localhost:27017/test                         | mongodb://localhost:27017/tradestore
            mongodb://localhost:27017/?replicaSet=rs0              | mongodb://localhost:27017/tradestore?replicaSet=rs0
            mongodb://user:secret@h1:27017,h2:27017/admin?ssl=true | mongodb://user:secret@h1:27017,h2:27017/tradestore?ssl=true
            mongodb+srv://cluster.example.com/?retryWrites=true    | mongodb+srv://cluster.example.com/tradestore?retryWrites=true
            """)
    void withDatabase_SetsDatabaseAndKeepsHostsCredentialsAndOptions(String connectionString, String expected) {
        assertThat(MongoLiquibaseMigration.withDatabase(connectionString, "tradestore")).isEqualTo(expected);
    }
}
