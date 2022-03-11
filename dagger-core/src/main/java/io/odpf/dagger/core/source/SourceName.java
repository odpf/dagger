package io.odpf.dagger.core.source;

import com.google.gson.annotations.SerializedName;

import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA;
import static io.odpf.dagger.core.utils.Constants.STREAM_SOURCE_DETAILS_SOURCE_NAME_PARQUET;

public enum SourceName {
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_KAFKA)
    KAFKA,
    @SerializedName(STREAM_SOURCE_DETAILS_SOURCE_NAME_PARQUET)
    PARQUET
}