package com.gojek.daggers.async.decorator.async;

import com.gojek.daggers.async.connector.HttpAsyncConnector;
import com.gojek.daggers.postprocessor.configs.HttpExternalSourceConfig;
import com.gojek.de.stencil.StencilClient;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

public class HttpDecorator implements AsyncDecorator {
    private HttpExternalSourceConfig httpExternalSourceConfig;
    private StencilClient stencilClient;
    private Integer asyncIOCapacity;
    private String type;
    private String[] columnNames;

    public HttpDecorator(HttpExternalSourceConfig httpExternalSourceConfig, StencilClient stencilClient, Integer asyncIOCapacity,
                         String type, String[] columnNames) {
        this.httpExternalSourceConfig = httpExternalSourceConfig;
        this.stencilClient = stencilClient;
        this.asyncIOCapacity = asyncIOCapacity;
        this.type = type;
        this.columnNames = columnNames;
    }

    @Override
    public Boolean canDecorate() {
        return type.equals("http");
    }

    @Override
    public Integer getAsyncIOCapacity() {
        return asyncIOCapacity;
    }

    @Override
    public AsyncFunction getAsyncFunction() {
        return new HttpAsyncConnector(columnNames, httpExternalSourceConfig, stencilClient);
    }

    @Override
    public Integer getStreamTimeout() {
        return Integer.valueOf(httpExternalSourceConfig.getStreamTimeout());
    }
}
