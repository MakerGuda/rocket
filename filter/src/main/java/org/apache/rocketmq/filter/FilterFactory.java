package org.apache.rocketmq.filter;

import java.util.HashMap;
import java.util.Map;

public class FilterFactory {

    public static final FilterFactory INSTANCE = new FilterFactory();

    protected static final Map<String, FilterSpi> FILTER_SPI_HOLDER = new HashMap<>(4);

    static {
        FilterFactory.INSTANCE.register(new SqlFilter());
    }

    /**
     * Register a filter.
     * Filter registered will be used in broker server, so take care of it's reliability and performance.</li>
     */
    public void register(FilterSpi filterSpi) {
        if (FILTER_SPI_HOLDER.containsKey(filterSpi.ofType())) {
            throw new IllegalArgumentException(String.format("Filter spi type(%s) already exist!", filterSpi.ofType()));
        }
        FILTER_SPI_HOLDER.put(filterSpi.ofType(), filterSpi);
    }

    /**
     * Get a filter registered, null if none exist.
     */
    public FilterSpi get(String type) {
        return FILTER_SPI_HOLDER.get(type);
    }

}