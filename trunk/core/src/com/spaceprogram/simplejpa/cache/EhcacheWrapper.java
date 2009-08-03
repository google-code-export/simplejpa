package com.spaceprogram.simplejpa.cache;

import net.sf.ehcache.hibernate.EhCache;
import net.sf.ehcache.*;
import net.sf.ehcache.Cache;
import net.sf.ehcache.store.Policy;
import net.sf.ehcache.exceptionhandler.CacheExceptionHandler;
import net.sf.ehcache.extension.CacheExtension;
import net.sf.ehcache.bootstrap.BootstrapCacheLoader;
import net.sf.ehcache.event.RegisteredEventListeners;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.loader.CacheLoader;

import java.io.Serializable;
import java.util.Map;
import java.util.Collection;
import java.util.List;

/**
 * User: treeder
 * Date: Aug 2, 2009
 * Time: 9:44:53 PM
 */
public class EhcacheWrapper implements Ehcache, com.spaceprogram.simplejpa.cache.Cache {
    private Cache cache;

    public EhcacheWrapper(Cache cache) {
        this.cache = cache;
    }

    public void initialise() {
        cache.initialise();
    }

    public void bootstrap() {
        cache.bootstrap();
    }

    public void put(Element element) throws IllegalArgumentException, IllegalStateException, CacheException {
        cache.put(element);
    }

    public void put(Element element, boolean b) throws IllegalArgumentException, IllegalStateException, CacheException {
        cache.put(element, b);
    }

    public void putQuiet(Element element) throws IllegalArgumentException, IllegalStateException, CacheException {
        cache.putQuiet(element);
    }

    public Element get(Serializable serializable) throws IllegalStateException, CacheException {
        return cache.get(serializable);
    }

    public int size() {
        return getSize();
    }

    public Element get(Object o) throws IllegalStateException, CacheException {
        return cache.get(o);
    }

    public void put(Object o, Object o1) {
        cache.put(new Element(o, o1));
    }

    public Element getWithLoader(Object o, CacheLoader cacheLoader, Object o1) throws CacheException {
        return cache.getWithLoader(o, cacheLoader, o1);
    }

    public void load(Object o) throws CacheException {
        cache.load(o);
    }

    public Map getAllWithLoader(Collection collection, Object o) throws CacheException {
        return cache.getAllWithLoader(collection, o);
    }

    public void loadAll(Collection collection, Object o) throws CacheException {
        cache.loadAll(collection, o);
    }

    public Element getQuiet(Serializable serializable) throws IllegalStateException, CacheException {
        return cache.getQuiet(serializable);
    }

    public Element getQuiet(Object o) throws IllegalStateException, CacheException {
        return cache.getQuiet(o);
    }

    public List getKeys() throws IllegalStateException, CacheException {
        return cache.getKeys();
    }

    public List getKeysWithExpiryCheck() throws IllegalStateException, CacheException {
        return cache.getKeysWithExpiryCheck();
    }

    public List getKeysNoDuplicateCheck() throws IllegalStateException {
        return cache.getKeysNoDuplicateCheck();
    }

    public boolean remove(Serializable serializable) throws IllegalStateException {
        return cache.remove(serializable);
    }

    public boolean remove(Object o) throws IllegalStateException {
        return cache.remove(o);
    }

    public void clear() {

    }

    public boolean remove(Serializable serializable, boolean b) throws IllegalStateException {
        return cache.remove(serializable, b);
    }

    public boolean remove(Object o, boolean b) throws IllegalStateException {
        return cache.remove(o, b);
    }

    public boolean removeQuiet(Serializable serializable) throws IllegalStateException {
        return cache.removeQuiet(serializable);
    }

    public boolean removeQuiet(Object o) throws IllegalStateException {
        return cache.removeQuiet(o);
    }

    public void removeAll() throws IllegalStateException, CacheException {
        cache.removeAll();
    }

    public void removeAll(boolean b) throws IllegalStateException, CacheException {
        cache.removeAll(b);
    }

    public void dispose() throws IllegalStateException {
        cache.dispose();
    }

    public CacheConfiguration getCacheConfiguration() {
        return cache.getCacheConfiguration();
    }

    public void flush() throws IllegalStateException, CacheException {
        cache.flush();
    }

    public int getSize() throws IllegalStateException, CacheException {
        return cache.getSize();
    }

    public long calculateInMemorySize() throws IllegalStateException, CacheException {
        return cache.calculateInMemorySize();
    }

    public long getMemoryStoreSize() throws IllegalStateException {
        return cache.getMemoryStoreSize();
    }

    public int getDiskStoreSize() throws IllegalStateException {
        return cache.getDiskStoreSize();
    }

    public Status getStatus() {
        return cache.getStatus();
    }

    public String getName() {
        return cache.getName();
    }

    public void setName(String s) throws IllegalArgumentException {
        cache.setName(s);
    }

    public String toString() {
        return cache.toString();
    }

    public boolean isExpired(Element element) throws IllegalStateException, NullPointerException {
        return cache.isExpired(element);
    }

    public Object clone() throws CloneNotSupportedException {
        return cache.clone();
    }

    public RegisteredEventListeners getCacheEventNotificationService() {
        return cache.getCacheEventNotificationService();
    }

    public boolean isElementInMemory(Serializable serializable) {
        return cache.isElementInMemory(serializable);
    }

    public boolean isElementInMemory(Object o) {
        return cache.isElementInMemory(o);
    }

    public boolean isElementOnDisk(Serializable serializable) {
        return cache.isElementOnDisk(serializable);
    }

    public boolean isElementOnDisk(Object o) {
        return cache.isElementOnDisk(o);
    }

    public String getGuid() {
        return cache.getGuid();
    }

    public CacheManager getCacheManager() {
        return cache.getCacheManager();
    }

    public void clearStatistics() throws IllegalStateException {
        cache.clearStatistics();
    }

    public int getStatisticsAccuracy() {
        return cache.getStatisticsAccuracy();
    }

    public void setStatisticsAccuracy(int i) {
        cache.setStatisticsAccuracy(i);
    }

    public void evictExpiredElements() {
        cache.evictExpiredElements();
    }

    public boolean isKeyInCache(Object o) {
        return cache.isKeyInCache(o);
    }

    public boolean isValueInCache(Object o) {
        return cache.isValueInCache(o);
    }

    public Statistics getStatistics() throws IllegalStateException {
        return cache.getStatistics();
    }

    public void setCacheManager(CacheManager cacheManager) {
        cache.setCacheManager(cacheManager);
    }

    public BootstrapCacheLoader getBootstrapCacheLoader() {
        return cache.getBootstrapCacheLoader();
    }

    public void setBootstrapCacheLoader(BootstrapCacheLoader bootstrapCacheLoader) throws CacheException {
        cache.setBootstrapCacheLoader(bootstrapCacheLoader);
    }

    public void setDiskStorePath(String s) throws CacheException {
        cache.setDiskStorePath(s);
    }

    public boolean equals(Object o) {
        return cache.equals(o);
    }

    public int hashCode() {
        return cache.hashCode();
    }

    public void registerCacheExtension(CacheExtension cacheExtension) {
        cache.registerCacheExtension(cacheExtension);
    }

    public List<CacheExtension> getRegisteredCacheExtensions() {
        return cache.getRegisteredCacheExtensions();
    }

    public void unregisterCacheExtension(CacheExtension cacheExtension) {
        cache.unregisterCacheExtension(cacheExtension);
    }

    public float getAverageGetTime() {
        return cache.getAverageGetTime();
    }

    public void setCacheExceptionHandler(CacheExceptionHandler cacheExceptionHandler) {
        cache.setCacheExceptionHandler(cacheExceptionHandler);
    }

    public CacheExceptionHandler getCacheExceptionHandler() {
        return cache.getCacheExceptionHandler();
    }

    public void registerCacheLoader(CacheLoader cacheLoader) {
        cache.registerCacheLoader(cacheLoader);
    }

    public void unregisterCacheLoader(CacheLoader cacheLoader) {
        cache.unregisterCacheLoader(cacheLoader);
    }

    public List<CacheLoader> getRegisteredCacheLoaders() {
        return cache.getRegisteredCacheLoaders();
    }

    public boolean isDisabled() {
        return cache.isDisabled();
    }

    public void setDisabled(boolean b) {
        cache.setDisabled(b);
    }

    public Policy getMemoryStoreEvictionPolicy() {
        return cache.getMemoryStoreEvictionPolicy();
    }

    public void setMemoryStoreEvictionPolicy(Policy policy) {
        cache.setMemoryStoreEvictionPolicy(policy);
    }
}
