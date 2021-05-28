package io.odpf.dagger.core.processors.external.grpc;

import io.odpf.dagger.core.processors.common.DescriptorManager;
import io.odpf.dagger.core.processors.common.PostResponseTelemetry;
import io.odpf.dagger.core.processors.common.RowManager;
import io.odpf.dagger.core.processors.external.AsyncConnector;
import io.odpf.dagger.core.processors.external.ExternalMetricConfig;
import io.odpf.dagger.core.processors.external.SchemaConfig;
import io.odpf.dagger.core.processors.external.grpc.client.GrpcRequestHandler;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import io.odpf.dagger.core.exception.ChannelNotAvailableException;
import io.odpf.dagger.common.exceptions.DescriptorNotFoundException;
import io.odpf.dagger.core.exception.InvalidGrpcBodyException;
import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.core.metrics.aspects.ExternalSourceAspects;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.dagger.core.processors.external.grpc.client.GrpcClient;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.odpf.dagger.core.utils.Constants;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcAsyncConnector extends AsyncConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcAsyncConnector.class.getName());

    private GrpcSourceConfig grpcSourceConfig;

    private GrpcClient grpcClient;

    public GrpcAsyncConnector(GrpcSourceConfig grpcSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig,
                              GrpcClient grpcClient, ErrorReporter errorReporter, MeterStatsManager meterStatsManager, DescriptorManager descriptorManager) {
        this(grpcSourceConfig, externalMetricConfig, schemaConfig);
        this.grpcClient = grpcClient;
        setErrorReporter(errorReporter);
        setMeterStatsManager(meterStatsManager);
        setDescriptorManager(descriptorManager);
    }

    public GrpcAsyncConnector(GrpcSourceConfig grpcSourceConfig, ExternalMetricConfig externalMetricConfig, SchemaConfig schemaConfig) {
        super(Constants.GRPC_TYPE, grpcSourceConfig, externalMetricConfig, schemaConfig);
        this.grpcSourceConfig = grpcSourceConfig;

    }

    @Override
    public DescriptorManager initDescriptorManager(SchemaConfig schemaConfig) {
        return new DescriptorManager(schemaConfig.getStencilClientOrchestrator(), grpcSourceConfig.getGrpcStencilUrl());
    }

    @Override
    protected void createClient() {

        if (this.grpcClient == null) {
            this.grpcClient = new GrpcClient(grpcSourceConfig);
            this.grpcClient.addChannel();
        }
    }

    @Override
    protected void process(Row input, ResultFuture<Row> resultFuture) throws Exception {

        try {

            RowManager rowManager = new RowManager(input);

            Object[] requestVariablesValues = getEndpointHandler()
                    .getEndpointOrQueryVariablesValues(rowManager, resultFuture);
            if (getEndpointHandler().isQueryInvalid(resultFuture, rowManager, requestVariablesValues)) {
                return;
            }

            GrpcRequestHandler grpcRequestHandler = new GrpcRequestHandler(grpcSourceConfig, getDescriptorManager());
            DynamicMessage message = grpcRequestHandler.create(requestVariablesValues);

            GrpcResponseHandler grpcResponseHandler = new GrpcResponseHandler(grpcSourceConfig, getMeterStatsManager(),
                    rowManager, getColumnNameManager(), getOutputDescriptor(resultFuture), resultFuture, getErrorReporter(), new PostResponseTelemetry());

            grpcResponseHandler.startTimer();

            this.grpcClient.asyncUnaryCall(message, grpcResponseHandler, getInputDescriptorForGrpcRequest(resultFuture), getOutputDescriptorForGrpcResponse(resultFuture));
        } catch (InvalidGrpcBodyException e) {
            getMeterStatsManager().markEvent(ExternalSourceAspects.INVALID_CONFIGURATION);
            resultFuture.completeExceptionally(e);
        } catch (ChannelNotAvailableException e) {
            getMeterStatsManager().markEvent(ExternalSourceAspects.GRPC_CHANNEL_NOT_AVAILABLE);
            resultFuture.completeExceptionally(e);
        }
    }


    private Descriptors.Descriptor getOutputDescriptorForGrpcResponse(ResultFuture<Row> resultFuture) {
        String descriptorClassName = grpcSourceConfig.getGrpcResponseProtoSchema();
        Descriptors.Descriptor grpcProtoDescriptor = null;
        if (StringUtils.isNotEmpty(descriptorClassName)) {
            try {
                grpcProtoDescriptor = getDescriptorManager().getDescriptor(descriptorClassName);
            } catch (DescriptorNotFoundException descriptorNotFound) {
                reportAndThrowError(resultFuture, descriptorNotFound);
            }
        }
        return grpcProtoDescriptor;
    }

    private Descriptors.Descriptor getInputDescriptorForGrpcRequest(ResultFuture<Row> resultFuture) {
        String descriptorClassName = grpcSourceConfig.getGrpcRequestProtoSchema();
        Descriptors.Descriptor grpcProtoDescriptor = null;
        if (StringUtils.isNotEmpty(descriptorClassName)) {
            try {
                grpcProtoDescriptor = getDescriptorManager().getDescriptor(descriptorClassName);
            } catch (DescriptorNotFoundException descriptorNotFound) {
                reportAndThrowError(resultFuture, descriptorNotFound);
            }
        }
        return grpcProtoDescriptor;
    }

    @Override
    public void close() {
        grpcClient.close();
        grpcClient = null;
        getMeterStatsManager().markEvent(ExternalSourceAspects.CLOSE_CONNECTION_ON_EXTERNAL_CLIENT);
        LOGGER.info("GRPC Connector : Connection closed");
    }

    public GrpcClient getGrpcClient() {
        return grpcClient;
    }
}
