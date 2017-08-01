package com.gojek.daggers.config.commandline;

import com.gojek.daggers.config.CommandlineConfigurationProvider;
import org.apache.flink.configuration.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CommandlineConfigurationProviderTest {

    @Test
    public void shouldProvideFromEmptyInput() throws Exception {
        Configuration configurations = new CommandlineConfigurationProvider(new String[]{}).get();

        assertTrue(configurations.keySet().size() == 0);
    }

    @Test
    public void shouldProvideFromOneValidInput() throws Exception {
        Configuration configurations = new CommandlineConfigurationProvider(new String[]{"--key", "value"}).get();

        assertEquals(1, configurations.keySet().size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.getString("key", "").equals("value"));
    }

    @Test
    public void shouldProvideFromMultipleValidInputs() throws Exception {
        Configuration configurations = new CommandlineConfigurationProvider(new String[]{"--key", "value", "--k", "v"}).get();

        assertEquals(2, configurations.keySet().size());

        assertTrue(configurations.containsKey("key"));
        assertTrue(configurations.getString("key", "").equals("value"));
        assertTrue(configurations.containsKey("k"));
        assertTrue(configurations.getString("k", "").equals("v"));
    }
}
