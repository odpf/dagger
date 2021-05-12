package io.odpf.dagger.integrationtest;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.odpf.dagger.common.core.StencilClientOrchestrator;
import io.odpf.dagger.common.core.StreamInfo;
import io.odpf.dagger.core.processors.PostProcessorFactory;
import io.odpf.dagger.core.processors.telemetry.processor.MetricsTelemetryExporter;
import io.odpf.dagger.core.processors.types.PostProcessor;
import io.odpf.dagger.core.utils.Constants;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HttpExternalPostProcessorIntegrationTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(8089);

    private StencilClientOrchestrator stencilClientOrchestrator;
    private MetricsTelemetryExporter telemetryExporter = new MetricsTelemetryExporter();
    private Configuration configuration = new Configuration();

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void setUp() {

        String streams = "[{\"TOPIC_NAMES\":\"dummy-topic\",\"TABLE_NAME\":\"testbooking\",\"PROTO_CLASS_NAME\":\"io.odpf.dagger.consumer.TestBookingLogMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"localhost:6668\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-consumer\",\"STREAM_NAME\":\"localkafka\"}]";

        configuration.setString(Constants.POST_PROCESSOR_ENABLED_KEY, "true");
        configuration.setString(Constants.INPUT_STREAMS, streams);
    }

    @After
    public void tearDown() {
        wireMockRule.stop();
    }

    @Test
    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithCorrespondingDataTypeIfTypeGiven() throws Exception {

        String postProcessorConfigString =
                "{\n"
                + "  \"external_source\": {\n"
                + "    \"http\": [\n"
                + "      {\n"
                + "        \"endpoint\": \"http://localhost:8089\",\n"
                + "        \"verb\": \"get\",\n"
                + "        \"request_pattern\": \"/customer/%s\",\n"
                + "        \"request_variables\": \"customer_id\",\n"
                + "        \"stream_timeout\": \"5000\",\n"
                + "        \"connect_timeout\": \"5000\",\n"
                + "        \"fail_on_errors\": \"false\", \n"
                + "        \"capacity\": \"30\",\n"
                + "        \"headers\": {\n"
                + "          \"content-type\": \"application/json\"\n"
                + "        },\n"
                + "        \"type\": \"io.odpf.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                + "        \"output_mapping\": {\n"
                + "          \"surge_factor\": {\n"
                + "            \"path\": \"$.data\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        stubFor(get(urlEqualTo("/customer/123"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
    }

    @Test
    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithCorrespondingDataTypeIfTypeNotGiven() throws Exception {
        String postProcessorConfigString =
                "{\n"
                + "  \"external_source\": {\n"
                + "    \"http\": [\n"
                + "      {\n"
                + "        \"endpoint\": \"http://localhost:8089\",\n"
                + "        \"verb\": \"get\",\n"
                + "        \"request_pattern\": \"/customer/%s\",\n"
                + "        \"request_variables\": \"customer_id\",\n"
                + "        \"stream_timeout\": \"5000\",\n"
                + "        \"connect_timeout\": \"5000\",\n"
                + "        \"fail_on_errors\": \"false\", \n"
                + "        \"retain_response_type\": \"true\", \n"
                + "        \"capacity\": \"30\",\n"
                + "        \"headers\": {\n"
                + "          \"content-type\": \"application/json\"\n"
                + "        },\n"
                + "        \"output_mapping\": {\n"
                + "          \"surge_factor\": {\n"
                + "            \"path\": \"$.data\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        stubFor(get(urlEqualTo("/customer/123"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "dummy-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33, (Double) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0);
    }

    @Test
    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithExternalAndInternalSource() throws Exception {
        String postProcessorConfigWithInternalSourceString =
                "{\n"
                + "  \"external_source\": {\n"
                + "    \"http\": [\n"
                + "      {\n"
                + "        \"endpoint\": \"http://localhost:8089\",\n"
                + "        \"verb\": \"get\",\n"
                + "        \"request_pattern\": \"/customer/%s\",\n"
                + "        \"request_variables\": \"customer_id\",\n"
                + "        \"stream_timeout\": \"5000\",\n"
                + "        \"connect_timeout\": \"5000\",\n"
                + "        \"fail_on_errors\": \"false\", \n"
                + "        \"capacity\": \"30\",\n"
                + "        \"headers\": {\n"
                + "          \"content-type\": \"application/json\"\n"
                + "        },\n"
                + "        \"type\": \"io.odpf.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                + "        \"output_mapping\": {\n"
                + "          \"surge_factor\": {\n"
                + "            \"path\": \"$.data\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "    \"internal_source\": [\n"
                + "       {"
                + "       \"output_field\": \"event_timestamp\", \n"
                + "       \"type\": \"function\",\n"
                + "       \"value\": \"CURRENT_TIMESTAMP\"\n"
                + "       }"
                + "   ]"
                + "}";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithInternalSourceString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        stubFor(get(urlEqualTo("/customer/123"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "random-order-id");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
    }

    @Test
    public void shouldPopulateFieldFromHTTPGetApiOnSuccessResponseWithAllThreeSourcesIncludingTransformer() throws Exception {
        String postProcessorConfigWithTransformerString =
                "{\n"
                + "  \"external_source\": {\n"
                + "    \"http\": [\n"
                + "      {\n"
                + "        \"endpoint\": \"http://localhost:8089\",\n"
                + "        \"verb\": \"get\",\n"
                + "        \"request_pattern\": \"/customer/%s\",\n"
                + "        \"request_variables\": \"customer_id\",\n"
                + "        \"stream_timeout\": \"5000\",\n"
                + "        \"connect_timeout\": \"5000\",\n"
                + "        \"fail_on_errors\": \"false\", \n"
                + "        \"retain_response_type\": \"true\", \n"
                + "        \"capacity\": \"30\",\n"
                + "        \"headers\": {\n"
                + "          \"content-type\": \"application/json\"\n"
                + "        },\n"
                + "        \"output_mapping\": {\n"
                + "          \"features\": {\n"
                + "            \"path\": \"$.data\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "    \"internal_source\": [\n"
                + "       {"
                + "       \"output_field\": \"event_timestamp\", \n"
                + "       \"type\": \"function\",\n"
                + "       \"value\": \"CURRENT_TIMESTAMP\"\n"
                + "       },"
                + "       {"
                + "       \"output_field\": \"customer_id\", \n"
                + "       \"type\": \"sql\",\n"
                + "       \"value\": \"customer_id\"\n"
                + "       }"
                + "   ]"
                + ",\n"
                + "   \"transformers\": ["
                + "     {\n"
                + "       \"transformation_class\": \"io.odpf.dagger.functions.transformers.ClearColumnTransformer\",\n"
                + "       \"transformation_arguments\": {\n"
                + "         \"targetColumnName\": \"customer_id\"\n"
                + "       }\n"
                + "     }\n"
                + "   ] \n"
                + " }";

        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigWithTransformerString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        stubFor(get(urlEqualTo("/customer/123"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"order_id", "customer_id", "driver_id"};
        Row inputData = new Row(3);
        inputData.setField(0, "RB-123");
        inputData.setField(1, "123");
        inputData.setField(2, "456");

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33, CollectSink.OUTPUT_VALUES.get(0).getField(0));
        assertTrue(CollectSink.OUTPUT_VALUES.get(0).getField(1) instanceof Timestamp);
        assertEquals("", CollectSink.OUTPUT_VALUES.get(0).getField(2));
    }

    @Test
    public void shouldPopulateFieldsFromHttpPostApiWithProperJsonBodyForComplexDataTypes() throws Exception {
        String streams = "[{\"TOPIC_NAMES\":\"dummy-topic\",\"TABLE_NAME\":\"testbooking\",\"PROTO_CLASS_NAME\":\"io.odpf.dagger.consumer.TestBookingLogMessage\",\"EVENT_TIMESTAMP_FIELD_INDEX\":\"41\",\"KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"localhost:6668\",\"KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"\",\"KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"test-consumer\",\"STREAM_NAME\":\"localkafka\"}]";
        configuration.setString(Constants.INPUT_STREAMS, streams);
        String postProcessorConfigString =
                "{\n"
                + "  \"external_source\": {\n"
                + "    \"http\": [\n"
                + "      {\n"
                + "        \"endpoint\": \"http://localhost:8089\",\n"
                + "        \"verb\": \"POST\",\n"
                + "        \"request_pattern\": \"{\\\"test\\\": %s}\",\n"
                + "        \"request_variables\": \"meta_array\",\n"
                + "        \"stream_timeout\": \"5000\",\n"
                + "        \"connect_timeout\": \"5000\",\n"
                + "        \"fail_on_errors\": \"false\", \n"
                + "        \"capacity\": \"30\",\n"
                + "        \"headers\": {\n"
                + "          \"content-type\": \"application/json\"\n"
                + "        },\n"
                + "        \"type\": \"io.odpf.dagger.consumer.TestSurgeFactorLogMessage\", \n"
                + "        \"output_mapping\": {\n"
                + "          \"surge_factor\": {\n"
                + "            \"path\": \"$.data\"\n"
                + "          }\n"
                + "        }\n"
                + "      }\n"
                + "    ]\n"
                + "  }\n"
                + "}";
        configuration.setString(Constants.POST_PROCESSOR_CONFIG_KEY, postProcessorConfigString);
        stencilClientOrchestrator = new StencilClientOrchestrator(configuration);

        stubFor(post(urlEqualTo("/"))
                .withRequestBody(equalToJson("{\"test\": [\"meta_0\",\"meta_1\"]}"))
                .withHeader("content-type", equalTo("application/json"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{data: 23.33}")));


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CollectSink.OUTPUT_VALUES.clear();

        String[] inputColumnNames = new String[]{"meta_array"};
        Row inputData = new Row(2);
        inputData.setField(0, new String[]{"meta_0", "meta_1"});

        DataStream<Row> dataStream = env.fromElements(Row.class, inputData);
        StreamInfo streamInfo = new StreamInfo(dataStream, inputColumnNames);

        StreamInfo postProcessedStreamInfo = addPostProcessor(streamInfo);
        postProcessedStreamInfo.getDataStream().addSink(new CollectSink());

        env.execute();
        assertEquals(23.33F, (Float) CollectSink.OUTPUT_VALUES.get(0).getField(0), 0.0F);
    }

    private static class CollectSink implements SinkFunction<Row> {

        static final List<Row> OUTPUT_VALUES = new ArrayList<>();

        @Override
        public synchronized void invoke(Row inputRow, Context context) {
            OUTPUT_VALUES.add(inputRow);
        }
    }

    private StreamInfo addPostProcessor(StreamInfo streamInfo) {
        List<PostProcessor> postProcessors = PostProcessorFactory.getPostProcessors(configuration, stencilClientOrchestrator, streamInfo.getColumnNames(), telemetryExporter);
        StreamInfo postProcessedStream = streamInfo;
        for (PostProcessor postProcessor : postProcessors) {
            postProcessedStream = postProcessor.process(postProcessedStream);
        }
        return postProcessedStream;
    }
}
