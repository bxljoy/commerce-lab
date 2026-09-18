package com.commercelab.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class InventoryConfigurationTest {

    @Test
    void standaloneDatasourceUsesComposeDatabase() throws IOException {
        Map<String, Object> application = loadClasspathYaml("application.yml");
        Map<String, Object> compose = loadYaml(repositoryRoot().resolve("docker-compose.yml"));

        String datasourceUrl = nestedString(application, "spring", "datasource", "url");
        String defaultDatabase = datasourceUrl.substring(
                datasourceUrl.lastIndexOf('/') + 1, datasourceUrl.length() - 1);
        String composeDatabase = nestedString(
                compose, "services", "inventory-postgres", "environment", "POSTGRES_DB");

        assertThat(defaultDatabase).isEqualTo(composeDatabase);
    }

    private static Map<String, Object> loadClasspathYaml(String resource) throws IOException {
        try (InputStream input = InventoryConfigurationTest.class
                .getClassLoader()
                .getResourceAsStream(resource)) {
            if (input == null) {
                throw new IOException("Missing classpath resource: " + resource);
            }
            return new Yaml().load(input);
        }
    }

    private static Map<String, Object> loadYaml(Path path) throws IOException {
        try (InputStream input = Files.newInputStream(path)) {
            return new Yaml().load(input);
        }
    }

    private static Path repositoryRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null && !Files.exists(current.resolve("docker-compose.yml"))) {
            current = current.getParent();
        }
        if (current == null) {
            throw new IllegalStateException("Could not locate repository root");
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private static String nestedString(Map<String, Object> values, String... path) {
        Object current = values;
        for (String segment : path) {
            current = ((Map<String, Object>) current).get(segment);
        }
        return (String) current;
    }
}
