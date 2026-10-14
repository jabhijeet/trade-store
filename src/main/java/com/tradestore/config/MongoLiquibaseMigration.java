package com.tradestore.config;

import com.mongodb.ConnectionString;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.command.core.UpdateCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;

/**
 * Applies the MongoDB changelog with the Liquibase MongoDB extension. Spring Boot only auto-configures Liquibase
 * for JDBC databases, so this runs the same {@code update} command explicitly. It runs while the application
 * context is being created, before Kafka listeners and scheduled jobs start writing to MongoDB.
 */
@Slf4j
@RequiredArgsConstructor
public class MongoLiquibaseMigration implements InitializingBean {

    private static final String CLASSPATH_PREFIX = "classpath:";

    private final String connectionString;
    private final String databaseName;
    private final String changeLog;

    @Override
    public void afterPropertiesSet() throws Exception {
        String changeLogPath = changeLog.startsWith(CLASSPATH_PREFIX)
                ? changeLog.substring(CLASSPATH_PREFIX.length())
                : changeLog;
        log.info("Applying MongoDB changelog: database={}, changeLog={}", databaseName, changeLogPath);
        Scope.child(Scope.Attr.resourceAccessor.name(), new ClassLoaderResourceAccessor(getClass().getClassLoader()),
                () -> new CommandScope(UpdateCommandStep.COMMAND_NAME)
                        .addArgumentValue(DbUrlConnectionArgumentsCommandStep.URL_ARG,
                                withDatabase(connectionString, databaseName))
                        .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, changeLogPath)
                        .execute());
    }

    /**
     * Returns the connection string with {@code database} as its path, keeping hosts, credentials and options.
     * The Liquibase MongoDB extension reads the target database from the connection string, while Spring may take
     * it from separate configuration.
     */
    static String withDatabase(String connectionString, String database) {
        if (database.equals(new ConnectionString(connectionString).getDatabase())) {
            return connectionString;
        }
        int optionsStart = connectionString.indexOf('?');
        String withoutOptions = optionsStart < 0 ? connectionString : connectionString.substring(0, optionsStart);
        String options = optionsStart < 0 ? "" : connectionString.substring(optionsStart);
        int hostsStart = withoutOptions.indexOf("://") + "://".length();
        int pathStart = withoutOptions.indexOf('/', hostsStart);
        String schemeAndHosts = pathStart < 0 ? withoutOptions : withoutOptions.substring(0, pathStart);
        return schemeAndHosts + "/" + database + options;
    }
}
