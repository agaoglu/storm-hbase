package backtype.storm.contrib.hbase.trident;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import storm.trident.state.map.IBackingMap;

import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Useful to layer over a map that communicates with a database. you generally layer opaque map over this over your database store
 * @author nathan
 * @param <T>
 */
public class GuavaCachedMap<T> implements IBackingMap<T> {
    Cache<List<Object>, Optional<T>> _cache;
    IBackingMap<T> _delegate;

    public GuavaCachedMap(IBackingMap<T> delegate, int cacheSize) {
        _delegate = delegate;
        _cache = CacheBuilder
        		.newBuilder()
        		.initialCapacity(cacheSize / 10)
        		.maximumSize(cacheSize)
        		.expireAfterWrite(10, TimeUnit.MINUTES)
        		.build();
    }

    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        Map<List<Object>, T> results = new HashMap<List<Object>, T>();
        List<List<Object>> toGet = new ArrayList<List<Object>>();
        for(List<Object> key: keys) {
        	Optional<T> result = _cache.getIfPresent(key);
        	if (result != null) {
                results.put(key, result.orNull());
            } else {
                toGet.add(key);
            }
        }

        List<T> fetchedVals = _delegate.multiGet(toGet);
        for(int i=0; i<toGet.size(); i++) {
            List<Object> key = toGet.get(i);
            T val = fetchedVals.get(i);
            _cache.put(key, Optional.fromNullable(val));
            results.put(key, val);
        }

        List<T> ret = new ArrayList<T>(keys.size());
        for(List<Object> key: keys) {
            ret.add(results.get(key));
        }
        return ret;
    }

    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        cache(keys, values);
        _delegate.multiPut(keys, values);
    }

    private void cache(List<List<Object>> keys, List<T> values) {
        for(int i=0; i<keys.size(); i++) {
            _cache.put(keys.get(i), Optional.fromNullable(values.get(i)));
        }
    }

}
