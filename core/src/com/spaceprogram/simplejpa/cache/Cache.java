package com.spaceprogram.simplejpa.cache;

import net.sf.ehcache.CacheException;

import java.util.Map;
import java.util.Collection;

/**
 * User: treeder
 * Date: Aug 2, 2009
 * Time: 7:07:41 PM
 */
public interface Cache {
    int size();

    Object get(Object o);

    Map getAll(Collection collection) throws CacheException;

    void put(Object o, Object o1);

    Object remove(Object o);

    void clear();
}
