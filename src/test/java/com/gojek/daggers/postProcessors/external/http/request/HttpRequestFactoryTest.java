package com.gojek.daggers.postProcessors.external.http.request;

import com.gojek.daggers.exception.InvalidHttpVerbException;
import com.gojek.daggers.postProcessors.external.http.HttpSourceConfig;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class HttpRequestFactoryTest {

    @Mock
    private AsyncHttpClient httpClient;

    @Mock
    private BoundRequestBuilder request;

    private HttpSourceConfig httpSourceConfig;
    private ArrayList<Object> requestVariablesValues;

    @Before
    public void setup() {
        initMocks(this);
        requestVariablesValues = new ArrayList<>();
        requestVariablesValues.add(1);
    }

    @Test
    public void shouldReturnPostRequestOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "POST", "{\"key\": \"%s\"}", "1", "123", "234", false, "type", "345", new HashMap<>(), null);
        when(httpClient.preparePost("http://localhost:8080/test")).thenReturn(request);
        when(request.setBody("{\"key\": \"123456\"}")).thenReturn(request);
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray());

        verify(httpClient, times(1)).preparePost("http://localhost:8080/test");
        verify(httpClient, times(0)).prepareGet(any(String.class));
    }

    @Test
    public void shouldReturnGetRequestOnTheBasisOfConfiguration() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "GET", "/key/%s", "1", "123", "234", false, "type", "345", new HashMap<>(), null);
        when(httpClient.prepareGet("http://localhost:8080/test/key/1")).thenReturn(request);
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray());

        verify(httpClient, times(1)).prepareGet("http://localhost:8080/test/key/1");
        verify(httpClient, times(0)).preparePost(any(String.class));
    }

    @Test(expected = InvalidHttpVerbException.class)
    public void shouldThrowExceptionForUnsupportedHttpVerb() {
        httpSourceConfig = new HttpSourceConfig("http://localhost:8080/test", "PATCH", "/key/%s", "1", "123", "234", false, "type", "345", new HashMap<>(), null);
        HttpRequestFactory.createRequest(httpSourceConfig, httpClient, requestVariablesValues.toArray());
    }

}