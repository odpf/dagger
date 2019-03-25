package com.gojek.daggers.config.system;

import com.gojek.daggers.config.EnvironmentConfigurationProvider;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class EnvironmentConfigurationProviderTest {

    @Test
    public void shouldProvideSystemConfiguration() {
        HashMap<String, String> environmentParameters = new HashMap<String, String>() {{
            put("key", "value");
            put("key2", "value2");
        }};

        Configuration stringStringMap = new EnvironmentConfigurationProvider(environmentParameters).get();

        assertEquals(stringStringMap.getString("key", ""), "value");
        assertEquals(stringStringMap.getString("key2", ""), "value2");
    }
}
