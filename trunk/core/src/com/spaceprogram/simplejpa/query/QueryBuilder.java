package com.spaceprogram.simplejpa.query;

import javax.persistence.Query;
import javax.persistence.EntityManager;

/**
 * A helper class to build parameterized queries.
 *
 * http://code.google.com/p/simplejpa/wiki/JPAQuery
 * 
 * User: treeder
 * Date: Nov 7, 2008
 * Time: 5:25:23 PM
 */
public interface QueryBuilder {

    void append(String s);

    void append(String s, String parameterName, Object parameterValue);

    Query makeQuery(EntityManager em);
}
