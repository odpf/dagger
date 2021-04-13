package io.odpf.dagger.processors.external.es;

import io.odpf.dagger.metrics.MeterStatsManager;
import io.odpf.dagger.metrics.reporters.ErrorReporter;
import io.odpf.dagger.processors.external.AsyncConnector;
import io.odpf.dagger.processors.external.ExternalMetricConfig;
import io.odpf.dagger.processors.external.SchemaConfig;
import io.odpf.dagger.processors.common.PostResponseTelemetry;
import io.odpf.dagger.processors.common.RowManager;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.utils.Constants;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class EsAsyncConnector extends AsyncConnector {

    private final EsSourceConfig esSourceConfig;
    private RestClient esClient;

    EsAsyncConnector(EsSourceConfig esSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig,
                     RestClient esClient, ErrorReporter errorReporter, MeterStatsManager meterStatsManager) {
        this(esSourceConfig, externalMetricConfig, schemaConfig);
        this.esClient = esClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
    }

    public EsAsyncConnector(EsSourceConfig esSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        super(Constants.ES_TYPE, esSourceConfig, externalMetricConfig, schemaConfig);
        this.esSourceConfig = esSourceConfig;
    }

    @Override
    protected void createClient() {
        if (esClient == null) {
            esClient = RestClient.builder(
                    getHttpHosts()
            ).setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder
                            .setDefaultCredentialsProvider(getCredentialsProvider())
                            .setDefaultRequestConfig(getRequestConfig())).setMaxRetryTimeoutMillis(esSourceConfig.getRetryTimeout()).build();
        }
    }

    @Override
    protected void process(Row input, ResultFuture<Row> resultFuture) {
        RowManager rowManager = new RowManager(input);
        Object[] endpointVariablesValues = getEndpointHandler()
                .getEndpointOrQueryVariablesValues(rowManager, resultFuture);
        if (getEndpointHandler().isQueryInvalid(resultFuture, rowManager, endpointVariablesValues)) {
            return;
        }
        String esEndpoint = String.format(esSourceConfig.getPattern(), endpointVariablesValues);
        Request esRequest = new Request("GET", esEndpoint);
        EsResponseHandler esResponseHandler = new EsResponseHandler(esSourceConfig, getMeterStatsManager(), rowManager,
                getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());
        esResponseHandler.startTimer();
        esClient.performRequestAsync(esRequest, esResponseHandler);
    }

    private HttpHost[] getHttpHosts() {
        List<String> hosts = Arrays.asList(esSourceConfig.getHost().split(","));
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        hosts.forEach(s -> httpHosts.add(new HttpHost(s, esSourceConfig.getPort())));
        return httpHosts.toArray(new HttpHost[0]);
    }

    private RequestConfig getRequestConfig() {
        return RequestConfig.custom()
                .setConnectTimeout(esSourceConfig.getConnectTimeout())
                .setSocketTimeout(esSourceConfig.getSocketTimeout()).build();
    }

    private CredentialsProvider getCredentialsProvider() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(esSourceConfig.getUser(), esSourceConfig.getPassword()));
        return credentialsProvider;
    }
}
