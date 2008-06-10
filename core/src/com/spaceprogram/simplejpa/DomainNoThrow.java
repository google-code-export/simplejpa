package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.QueryResult;
import com.xerox.amazonws.sdb.SDBException;

import javax.persistence.PersistenceException;

/**
 * This will catches "The specified domain does not exist" exceptions and handles them cleanly without throwing.
 * We use this because we only create domains on a persist() operation
 *
 * User: treeder
 * Date: Mar 4, 2008
 * Time: 10:23:13 AM
 */
public class DomainNoThrow extends Domain {

    protected DomainNoThrow(String domainName, String awsAccessKeyId, String awsSecretAccessKey, boolean isSecure, String server) throws SDBException {
        super(domainName, awsAccessKeyId, awsSecretAccessKey, isSecure, server);
    }

    public QueryResult listItems() throws SDBException {
        try {
            return super.listItems();
        } catch (SDBException e) {
             if (e.getMessage() != null && e.getMessage().contains("The specified domain does not exist")) {
                 // todo: waiting on typica to make QueryResult constructor public
//                return new QueryResult(null, null); // no need to throw here
            }
            throw new PersistenceException(e);
        }
    }

    public QueryResult listItems(String queryString) throws SDBException {
        return super.listItems(queryString);
    }

    public QueryResult listItems(String queryString, String nextToken) throws SDBException {
        return super.listItems(queryString, nextToken);
    }

    public QueryResult listItems(String queryString, String nextToken, int maxResults) throws SDBException {
        return super.listItems(queryString, nextToken, maxResults);
    }
}
