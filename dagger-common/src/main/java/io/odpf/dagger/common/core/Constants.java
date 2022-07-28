package io.odpf.dagger.common.core;

public class Constants {
    public static final String SCHEMA_REGISTRY_STENCIL_ENABLE_KEY = "SCHEMA_REGISTRY_STENCIL_ENABLE";
    public static final boolean SCHEMA_REGISTRY_STENCIL_ENABLE_DEFAULT = false;
    public static final String SCHEMA_REGISTRY_STENCIL_URLS_KEY = "SCHEMA_REGISTRY_STENCIL_URLS";
    public static final String SCHEMA_REGISTRY_STENCIL_URLS_DEFAULT = "";
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS = "SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS";
    public static final Integer SCHEMA_REGISTRY_STENCIL_FETCH_TIMEOUT_MS_DEFAULT = 60000;
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_KEY = "SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS";
    public static final String SCHEMA_REGISTRY_STENCIL_FETCH_HEADERS_DEFAULT = "";

    public static final String UDF_TELEMETRY_GROUP_KEY = "udf";
    public static final String GAUGE_ASPECT_NAME = "value";

    public static final long SLIDING_TIME_WINDOW = 10;
    public static final String STREAM_INPUT_SCHEMA_PROTO_CLASS = "INPUT_SCHEMA_PROTO_CLASS";
    public static final String STREAM_INPUT_SCHEMA_TABLE = "INPUT_SCHEMA_TABLE";
    public static final String INPUT_STREAMS = "STREAMS";

    public static final String INTERNAL_VALIDATION_FIELD_KEY = "__internal_validation_field__";
    public static final String ROWTIME = "rowtime";
}
