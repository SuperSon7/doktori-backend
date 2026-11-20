package com.example.doktoribackend.config;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileSystemResource;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RedisSentinelProfileTest {
    @ParameterizedTest
    @CsvSource({"api,false", "api,true", "chat,false", "chat,true"})
    void bindsActualProfileWithoutStartingServicesOrImportingAws(String module, boolean override) throws Exception {
        Path file = Path.of("..").resolve(module).resolve("src/main/resources/application-prod.yml");
        var sources = new YamlPropertySourceLoader().load(module, new FileSystemResource(file));
        var profile = sources.stream()
                .filter(source -> "redis-sentinel".equals(source.getProperty("spring.config.activate.on-profile")))
                .findFirst().orElseThrow();
        Map<String, Object> values = new HashMap<>();
        values.put("SPRING_REDIS_PASSWORD", "test-data-password");
        values.put("SPRING_REDIS_SENTINEL_MASTER", "doktori-master");
        values.put("SPRING_REDIS_SENTINEL_NODES", "sentinel-a:26379,sentinel-b:26379,sentinel-c:26379");
        if (override) values.put("SPRING_REDIS_SENTINEL_PASSWORD", "test-sentinel-password");
        var environment = new StandardEnvironment();
        environment.getPropertySources().addFirst(profile);
        environment.getPropertySources().addFirst(new MapPropertySource("test-only", values));
        var properties = Binder.get(environment).bind("spring.data.redis", RedisProperties.class).get();
        assertThat(properties.getSentinel().getMaster()).isEqualTo("doktori-master");
        assertThat(properties.getSentinel().getNodes()).hasSize(3);
        assertThat(properties.getSentinel().getPassword()).isEqualTo(override ? "test-sentinel-password" : "test-data-password");
    }
}
