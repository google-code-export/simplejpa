package com.spaceprogram.simplejpa.cache;

import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.event.CacheEventListener;

import java.util.Map;

/**
 * User: treeder
 * Date: Jun 7, 2008
 * Time: 4:46:11 PM
 */
public class NoopCacheFactory implements CacheFactory2 {
    private Cache noopCache = new NoopCache();

    public void init(Map properties) throws CacheException {

    }

    public Cache createCache(String name) {
        return noopCache;
    }

    public void shutdown() {

    }

    public void clearAll() {

    }

    public CacheManager getCacheManager() {
        return null;
    }

    public void addDefaultListener(CacheEventListener cacheEventListener) {

    }

    public Cache createCache(Map map) throws CacheException {
        return noopCache;
    }
}
