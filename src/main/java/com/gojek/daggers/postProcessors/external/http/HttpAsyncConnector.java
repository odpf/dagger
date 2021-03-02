package com.gojek.daggers.postProcessors.external.http;

import com.gojek.daggers.exception.InvalidHttpVerbException;
import com.gojek.daggers.metrics.MeterStatsManager;
import com.gojek.daggers.metrics.aspects.ExternalSourceAspects;
import com.gojek.daggers.metrics.reporters.ErrorReporter;
import com.gojek.daggers.postProcessors.external.AsyncConnector;
import com.gojek.daggers.postProcessors.external.ExternalMetricConfig;
import com.gojek.daggers.postProcessors.external.SchemaConfig;
import com.gojek.daggers.postProcessors.external.common.DescriptorManager;
import com.gojek.daggers.postProcessors.external.common.PostResponseTelemetry;
import com.gojek.daggers.postProcessors.external.common.RowManager;
import com.gojek.daggers.postProcessors.external.http.request.HttpRequestFactory;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.gojek.daggers.metrics.aspects.ExternalSourceAspects.CLOSE_CONNECTION_ON_EXTERNAL_CLIENT;
import static com.gojek.daggers.utils.Constants.HTTP_TYPE;
import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

public class HttpAsyncConnector extends AsyncConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpAsyncConnector.class.getName());
    private AsyncHttpClient httpClient;
    private HttpSourceConfig httpSourceConfig;

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig,
                              AsyncHttpClient httpClient, ErrorReporter errorReporter, MeterStatsManager meterStatsManager, DescriptorManager descriptorManager) {
        this(httpSourceConfig, externalMetricConfig, schemaConfig);
        this.httpClient = httpClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
        setDescriptorManager(descriptorManager);
    }

    public HttpAsyncConnector(HttpSourceConfig httpSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        super(HTTP_TYPE, httpSourceConfig, externalMetricConfig, schemaConfig);
        this.httpSourceConfig = httpSourceConfig;
    }

    AsyncHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    protected void createClient() {
        if (httpClient == null) {
            httpClient = asyncHttpClient(config().setConnectTimeout(httpSourceConfig.getConnectTimeout()));
        }
    }

    @Override
    public void close() throws Exception {
        httpClient.close();
        httpClient = null;
        getMeterStatsManager().markEvent(CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        LOGGER.error("HTTP Connector : Connection closed");
    }

    @Override
    protected void process(Row input, ResultFuture<Row> resultFuture) {
        try {
            RowManager rowManager = new RowManager(input);

            Object[] requestVariablesValues = getEndpointHandler()
                    .getEndpointOrQueryVariablesValues(rowManager, resultFuture);
            if (getEndpointHandler().isQueryInvalid(resultFuture, rowManager, requestVariablesValues)) {
                return;
            }

            BoundRequestBuilder request = HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues);
            HttpResponseHandler httpResponseHandler = new HttpResponseHandler(httpSourceConfig, getMeterStatsManager(),
                    rowManager, getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());
            httpResponseHandler.startTimer();
            request.execute(httpResponseHandler);
        } catch (InvalidHttpVerbException e) {
            getMeterStatsManager().markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(e);
        }

    }
}
