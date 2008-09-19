package com.spaceprogram.simplejpa.cache;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheFactory;

import java.util.Map;

/**
 * The JCache CacheFactory is pretty limited so added this one.
 *
 * User: treeder
 * Date: Jun 7, 2008
 * Time: 4:56:15 PM
 */
public interface CacheFactory2 extends CacheFactory {

    /**
     * Called once to load up the CacheManager.
     *
     * @param properties
     * @throws net.sf.jsr107cache.CacheException
     */
    void init(Map properties) throws CacheException;

    Cache createCache(String name) throws CacheException;

    void shutdown();

    void clearAll();

    CacheManager getCacheManager();

    /**
     * This allows you to add a cache listener that will be applied to every Cache that is created from this point on.
     * @param cacheEventListener
     */
    void addDefaultListener(CacheEventListener cacheEventListener);
}
