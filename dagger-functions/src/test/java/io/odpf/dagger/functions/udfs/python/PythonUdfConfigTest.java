package io.odpf.dagger.functions.udfs.python;

import io.odpf.dagger.common.configuration.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static io.odpf.dagger.functions.common.Constants.*;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class PythonUdfConfigTest {

    @Mock
    private Configuration configuration;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldParseConfig() {
        String pythonJsonConfig = "{ \"PYTHON_FILES\": \"/path/to/function.zip\", \"PYTHON_ARCHIVES\": \"/path/to/file.txt\", \"PYTHON_REQUIREMENTS\": \"requirements.txt\", \"PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE\": \"10000\", \"PYTHON_FN_EXECUTION_BUNDLE_SIZE\": \"100000\", \"PYTHON_FN_EXECUTION_BUNDLE_TIME\": \"1000\" }";

        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Assert.assertNotNull(pythonUdfConfig);
        Assert.assertEquals(pythonUdfConfig.getPythonFiles(), "/path/to/function.zip");
        Assert.assertEquals(pythonUdfConfig.getPythonArchives(), "/path/to/file.txt");
        Assert.assertEquals(pythonUdfConfig.getPythonRequirements(), "requirements.txt");
        Assert.assertEquals(pythonUdfConfig.getPythonArrowBatchSize(), 10000);
        Assert.assertEquals(pythonUdfConfig.getPythonBundleSize(), 100000);
        Assert.assertEquals(pythonUdfConfig.getPythonBundleTime(), 1000);
    }

    @Test
    public void shouldUseDefaultValueIfConfigIsNotGiven() {
        String pythonJsonConfig = "{ \"PYTHON_FILES\": \"/path/to/function.zip\", \"PYTHON_ARCHIVES\": \"/path/to/file.txt\", \"PYTHON_REQUIREMENTS\": \"requirements.txt\" }";

        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Assert.assertEquals(pythonUdfConfig.getPythonArrowBatchSize(), 10000);
        Assert.assertEquals(pythonUdfConfig.getPythonBundleSize(), 100000);
        Assert.assertEquals(pythonUdfConfig.getPythonBundleTime(), 1000);
    }

    @Test
    public void shouldReturnNullIfPythonFilesConfigIsNotGiven() {
        String pythonJsonConfig = "{\"PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE\": \"10000\", \"PYTHON_FN_EXECUTION_BUNDLE_SIZE\": \"100000\", \"PYTHON_FN_EXECUTION_BUNDLE_TIME\": \"1000\"}";

        when(configuration.getString(PYTHON_UDF_CONFIG, "")).thenReturn(pythonJsonConfig);
        PythonUdfConfig pythonUdfConfig = PythonUdfConfig.parse(configuration);

        Assert.assertNull(pythonUdfConfig.getPythonFiles());
        Assert.assertNull(pythonUdfConfig.getPythonArchives());
        Assert.assertNull(pythonUdfConfig.getPythonRequirements());
    }
}
