package io.odpf.dagger.functions.udfs.scalar;

import io.odpf.dagger.common.udfs.ScalarUdf;

import static com.github.davidmoten.geo.GeoHash.encodeHash;

public class GeoHash extends ScalarUdf {

    /**
     * Returns a geohash of given length for the given WGS84 point.
     *
     * @param latitude  the latitude
     * @param longitude the longitude
     * @param length    the length
     * @return the geohash string
     * @author Sumanth
     * @team DE
     */
    public String eval(double latitude, double longitude, int length) {
        return encodeHash(latitude, longitude, length);
    }
}
