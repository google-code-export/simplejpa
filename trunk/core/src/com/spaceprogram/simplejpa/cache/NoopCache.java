package com.spaceprogram.simplejpa.cache;

import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheListener;
import net.sf.jsr107cache.CacheException;
import net.sf.jsr107cache.CacheEntry;
import net.sf.jsr107cache.CacheStatistics;

import java.util.Set;
import java.util.Map;
import java.util.Collection;

/**
 * User: treeder
 * Date: Jun 7, 2008
 * Time: 4:47:01 PM
 */
public class NoopCache implements Cache {
    public boolean containsKey(Object o) {
        return false;
    }

    public boolean containsValue(Object o) {
        return false;
    }

    public Set entrySet() {
        return null;
    }

    public boolean isEmpty() {
        return false;
    }

    public Set keySet() {
        return null;
    }

    public void putAll(Map map) {

    }

    public int size() {
        return 0;
    }

    public Collection values() {
        return null;
    }

    public Object get(Object o) {
        return null;
    }

    public Map getAll(Collection collection) throws CacheException {
        return null;
    }

    public void load(Object o) throws CacheException {

    }

    public void loadAll(Collection collection) throws CacheException {

    }

    public Object peek(Object o) {
        return null;
    }

    public Object put(Object o, Object o1) {
        return null;
    }

    public CacheEntry getCacheEntry(Object o) {
        return null;
    }

    public CacheStatistics getCacheStatistics() {
        return null;
    }

    public Object remove(Object o) {
        return null;
    }

    public void clear() {

    }

    public void evict() {

    }

    public void addListener(CacheListener cacheListener) {

    }

    public void removeListener(CacheListener cacheListener) {

    }
}
