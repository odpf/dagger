package io.odpf.dagger.core.sink.bigquery;

import io.odpf.dagger.common.serde.proto.serialization.ProtoSerializer;
import io.odpf.dagger.core.exception.BigqueryWriterException;
import io.odpf.dagger.core.metrics.reporters.ErrorReporter;
import io.odpf.depot.OdpfSink;
import io.odpf.depot.OdpfSinkResponse;
import io.odpf.depot.error.ErrorInfo;
import io.odpf.depot.error.ErrorType;
import io.odpf.depot.exception.OdpfSinkException;
import io.odpf.depot.message.OdpfMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class BigquerySinkWriter implements SinkWriter<Row, Void, Void> {
    private final ProtoSerializer protoSerializer;
    private final OdpfSink bigquerySink;
    private final int batchSize;
    private final ErrorReporter errorReporter;
    private final Set<ErrorType> errorTypesForFailing;
    private final List<OdpfMessage> messages = new ArrayList<>();
    private int currentBatchSize;

    public BigquerySinkWriter(ProtoSerializer protoSerializer, OdpfSink bigquerySink, int batchSize, ErrorReporter errorReporter, Set<ErrorType> errorTypesForFailing) {
        this.protoSerializer = protoSerializer;
        this.bigquerySink = bigquerySink;
        this.batchSize = batchSize;
        this.errorReporter = errorReporter;
        this.errorTypesForFailing = errorTypesForFailing;
    }

    @Override
    public void write(Row element, Context context) throws IOException {
        log.info("adding row to BQ batch : " + element);
        byte[] key = protoSerializer.serializeKey(element);
        byte[] value = protoSerializer.serializeValue(element);
        OdpfMessage message = new OdpfMessage(key, value);
        if (currentBatchSize < batchSize) {
            messages.add(message);
            currentBatchSize++;
        }
        if (currentBatchSize >= batchSize) {
            pushToBq();
            messages.clear();
            currentBatchSize = 0;
        }
    }

    private void pushToBq() throws OdpfSinkException, BigqueryWriterException {
        log.info("Pushing " + currentBatchSize + " records to bq");
        OdpfSinkResponse odpfSinkResponse;
        try {
            odpfSinkResponse = bigquerySink.pushToSink(messages);
        } catch (Exception e) {
            errorReporter.reportFatalException(e);
            throw e;
        }
        if (odpfSinkResponse.hasErrors()) {
            logErrors(odpfSinkResponse, messages);
            checkAndThrow(odpfSinkResponse);
        }
    }

    protected void checkAndThrow(OdpfSinkResponse sinkResponse) throws BigqueryWriterException {
        Map<Boolean, List<ErrorInfo>> failedErrorTypes = sinkResponse.getErrors().values().stream().collect(
                Collectors.partitioningBy(errorInfo -> errorTypesForFailing.contains(errorInfo.getErrorType())));
        failedErrorTypes.get(Boolean.FALSE).forEach(errorInfo -> {
            errorReporter.reportNonFatalException(errorInfo.getException());
        });
        failedErrorTypes.get(Boolean.TRUE).forEach(errorInfo -> {
            errorReporter.reportFatalException(errorInfo.getException());
        });
        if (failedErrorTypes.get(Boolean.TRUE).size() > 0) {
            throw new BigqueryWriterException("Error occurred during writing to Bigquery");
        }
    }

    protected void logErrors(OdpfSinkResponse sinkResponse, List<OdpfMessage> sentMessages) {
        log.error("Failed to push " + sinkResponse.getErrors().size() + " records to BigquerySink");
        sinkResponse.getErrors().forEach((index, errorInfo) -> {
            OdpfMessage message = sentMessages.get(index.intValue());
            log.error("Failed to pushed message with metadata {}. The exception was {}. The ErrorType was {}",
                    message.getMetadataString(),
                    errorInfo.getException().getMessage(),
                    errorInfo.getErrorType().name());
        });

    }

    /**
     * This will be called before we checkpoint the Writer's state in Streaming execution mode.
     *
     * @param flush – Whether flushing the un-staged data or not
     * @return The data is ready to commit.
     * @throws IOException – if fail to prepare for a commit.
     */
    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException {
        if (flush) {
            pushToBq();
        }
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        bigquerySink.close();
    }

    @Override
    public List<Void> snapshotState(long checkpointId) {
        // We don't snapshot anything
        return Collections.emptyList();
    }
}
