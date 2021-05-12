package io.odpf.dagger.functions.udfs.scalar.dart.store.gcs;


import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.odpf.dagger.common.metrics.managers.GaugeStatsManager;
import io.odpf.dagger.functions.exceptions.BucketDoesNotExistException;
import io.odpf.dagger.functions.exceptions.TagDoesNotExistException;

import static io.odpf.dagger.functions.udfs.scalar.dart.DartAspects.DART_GCS_FILE_SIZE;
import static io.odpf.dagger.functions.udfs.scalar.dart.DartAspects.DART_GCS_PATH;

public class GcsClient {

    private Storage storage;

    private static final Double BYTES_TO_KB = 1024.0;
    private static final String DART_PATH = "DartPath";
    private static final String UDF_TELEMETRY_GROUP_KEY = "udf";

    public GcsClient(String projectId) {

        if (storage == null) {
            storage = StorageOptions.newBuilder()
                    .setProjectId(projectId)
                    .build().getService();

        }
    }

    public String fetchJsonData(String udfName, GaugeStatsManager gaugeStatsManager, String bucketName, String dartName) throws TagDoesNotExistException, BucketDoesNotExistException {

        Bucket bucket = storage.get(bucketName);
        if (bucket == null) {
            throw new BucketDoesNotExistException(String.format("Could not find the bucket in gcs for %s", dartName));
        }

        Blob blob = storage.get(BlobId.of(bucketName, dartName));

        if (blob == null) {
            throw new TagDoesNotExistException(String.format("Could not find the content in gcs for %s", dartName));
        }

        gaugeStatsManager.registerString(UDF_TELEMETRY_GROUP_KEY, udfName, DART_GCS_PATH.getValue(), dartName);
        gaugeStatsManager.registerDouble(DART_PATH, dartName, DART_GCS_FILE_SIZE.getValue(), blob.getContent().length / BYTES_TO_KB);

        return new String(blob.getContent());
    }
}
