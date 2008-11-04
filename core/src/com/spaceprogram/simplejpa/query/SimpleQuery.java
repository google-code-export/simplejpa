package com.spaceprogram.simplejpa.query;

import javax.persistence.Query;

/**
 * User: treeder
 * Date: Nov 2, 2008
 * Time: 12:50:20 AM
 */
public interface SimpleQuery extends Query {

    /**
     * Same as getSingleResult, but does not throw NonUniqueResultException or NoResultException
     * @return first result or null if no results.
     */
    Object getSingleResultNoThrow();
}
