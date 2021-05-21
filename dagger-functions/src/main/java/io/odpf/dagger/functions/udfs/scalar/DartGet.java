package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.metrics.managers.MeterStatsManager;
import io.odpf.dagger.common.udfs.ScalarUdf;
import io.odpf.dagger.functions.exceptions.KeyDoesNotExistException;
import io.odpf.dagger.functions.udfs.scalar.dart.DartAspects;
import io.odpf.dagger.functions.udfs.scalar.dart.store.gcs.GcsDataStore;
import io.odpf.dagger.functions.udfs.scalar.dart.types.MapCache;
import org.apache.flink.table.functions.FunctionContext;

import java.util.HashMap;
import java.util.Map;

import static io.odpf.dagger.common.core.Constants.UDF_TELEMETRY_GROUP_KEY;

public class DartGet extends ScalarUdf {
    private final GcsDataStore dataStore;
    private final Map<String, MapCache> cache;

    public DartGet(GcsDataStore dataStore) {
        this.dataStore = dataStore;
        cache = new HashMap<>();
    }

    public static DartGet withGcsDataStore(String projectId, String bucketId) {
        return new DartGet(new GcsDataStore(projectId, bucketId));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        MeterStatsManager meterStatsManager = new MeterStatsManager(context.getMetricGroup(), true);
        meterStatsManager.register(UDF_TELEMETRY_GROUP_KEY, this.getClass().getSimpleName(), DartAspects.values());
        dataStore.setMeterStatsManager(meterStatsManager);
        dataStore.setGaugeStatsManager(getGaugeStatsManager());
    }

    /**
     * To fetch a corresponding value in a collection given a key from data point.
     *
     * @param collectionName     the collection name
     * @param key                the key
     * @param refreshRateInHours ttl
     * @return the value in string
     * @author gaurav.s
     * @team DE
     */
    public String eval(String collectionName, String key, int refreshRateInHours) {
        if (cache.isEmpty() || !cache.containsKey(collectionName) || cache.get(collectionName).hasExpired(refreshRateInHours) || cache.get(collectionName).isEmpty()) {
            cache.put(collectionName, dataStore.getMap(collectionName));
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_GCS_FETCH_SUCCESS);
        }
        dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_HIT);
        return cache.get(collectionName).get(key);
    }

    public String eval(String collectionName, String key, int refreshRateInHours, String defaultValue) {
        try {
            return eval(collectionName, key, refreshRateInHours);
        } catch (KeyDoesNotExistException e) {
            dataStore.getMeterStatsManager().markEvent(DartAspects.DART_CACHE_MISS);
            return defaultValue;
        }
    }

}
