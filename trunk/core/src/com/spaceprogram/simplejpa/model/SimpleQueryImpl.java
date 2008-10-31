package com.spaceprogram.simplejpa.model;

import java.util.List;

/**
 * User: treeder
 * Date: Oct 18, 2008
 * Time: 4:39:13 PM
 */
public class SimpleQueryImpl implements SimpleQuery {
    private Class<?> aClass;

    public SimpleQueryImpl(Class<?> aClass) {

        this.aClass = aClass;
    }

    public SimpleQuery filter(String attribute, String comparison, String value) {
        return null;
    }

    public SimpleQuery order(String attribute, String direction) {
        return null;
    }

    public List getResultList() {
        return null;
    }
}
