package com.spaceprogram.simplejpa;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.SelectResult;
import com.spaceprogram.simplejpa.query.JPAQuery;
import com.spaceprogram.simplejpa.query.QueryImpl;
import org.apache.commons.collections.list.GrowthList;

import javax.persistence.PersistenceException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads objects in the list on demand from SimpleDB.
 * <p/>
 * <p/>
 * User: treeder Date: Feb 10, 2008 Time: 9:06:16 PM
 */
@SuppressWarnings("unchecked")
public class LazyList<E> extends AbstractList<E> implements Serializable {
    private static final int MAX_RESULTS_PER_REQUEST = 2500;

    private static Logger logger = Logger.getLogger(LazyList.class.getName());

    private transient EntityManagerSimpleJPA em;
    private Class genericReturnType;
    private QueryImpl origQuery;

    /**
     * Stores the actual objects for this list
     */
    private List<E> backingList;
    private String nextToken;
    private int count = -1;
    private String realQuery;
    private String domainName;
    private int maxResults = -1;

    public LazyList(EntityManagerSimpleJPA em, Class tClass, QueryImpl query) {
        this(em, tClass, query, -1);
    }

    public LazyList(EntityManagerSimpleJPA em, Class tClass, QueryImpl query, int maxResults) {
        this.em = em;
        this.genericReturnType = tClass;
        this.origQuery = query;
        AnnotationInfo ai = em.getAnnotationManager().getAnnotationInfo(genericReturnType);
        try {
        	domainName = em.getDomainName(ai.getRootClass());
            if (domainName == null) {
                logger.warning("Domain does not exist for " + ai.getRootClass());
                backingList = new GrowthList(0);
            } else {
                realQuery = query.createAmazonQuery().getValue();
                this.maxResults = maxResults;
            }
        }
        catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        if (count > -1)
            return count;

        if (backingList != null && nextToken == null) {
            count = backingList.size();
        } else {
            try {
                if (logger.isLoggable(Level.FINER))
                    logger.finer("Getting size.");
                JPAQuery queryClone = (JPAQuery) origQuery.getQ().clone();
                queryClone.setResult("count(*)");
                QueryImpl query2 = new QueryImpl(em, queryClone);
                query2.setParameters(origQuery.getParameters());
                query2.setForeignIds(origQuery.getForeignIds());
                List results = query2.getResultList();
                int resultCount = ((Long) results.get(0)).intValue();
                if (logger.isLoggable(Level.FINER))
                    logger.finer("Got:" + resultCount);

                if (maxResults > -1 && resultCount > maxResults) {
                    if (logger.isLoggable(Level.FINER))
                        logger.finer("Too much, adjusting to maxResults: " +
                                maxResults);
                    count = maxResults;
                } else {
                    count = resultCount;
                }
            }
            catch (CloneNotSupportedException e) {
                throw new PersistenceException(e);
            }
        }
        return count;
    }

    public void add(int index, E element) {
        backingList.add(index, element);
    }

    public E set(int index, E element) {
        return backingList.set(index, element);
    }

    public E remove(int index) {
        return backingList.remove(index);
    }

    public E get(int i) {
        if (logger.isLoggable(Level.FINER))
            logger.finer("getting from lazy list at index=" + i);
        loadAtleastItems(i);
        return backingList.get(i);
    }

    private synchronized void loadAtleastItems(int index) {
        if (backingList != null && nextToken == null) {
            return;
        }

        if (backingList == null) {
            backingList = new GrowthList();
        }

        while (backingList.size() <= index) {
            SelectResult qr;
            try {
                if (logger.isLoggable(Level.FINER))
                    logger.finer("query for lazylist=" + origQuery);

                int limit = maxResults - backingList.size();
                String limitQuery = realQuery
                        + " limit "
                        + (noLimit() ? MAX_RESULTS_PER_REQUEST :
                        (limit > MAX_RESULTS_PER_REQUEST ? MAX_RESULTS_PER_REQUEST
                                : limit));
                if (em.getFactory().isPrintQueries())
                    System.out.println("query in lazylist=" + limitQuery);
                qr = DomainHelper.selectItems(this.em.getSimpleDb(), limitQuery, nextToken);

                if (logger.isLoggable(Level.FINER))
                    logger.finer("got items for lazylist=" + qr.getItems().size());
                
                for (Item item : qr.getItems()) {
                    backingList.add((E) em.buildObject(genericReturnType, item.getName(), item.getAttributes()));
                }
                
                if (qr.getNextToken() == null || (!noLimit() && qr.getItems().size() == limit)) {
                    nextToken = null;
                    break;
                }

                if (!noLimit() && qr.getItems().size() > limit) {
                    throw new PersistenceException("Got more results than the limit.");
                }

                nextToken = qr.getNextToken();
            }
            catch (AmazonClientException e) {
                throw new PersistenceException("Query failed: Domain="
                        + domainName + " -> " + origQuery, e);
            }
        }

    }

    private boolean noLimit() {
        return maxResults == -1;

    }

    @Override
    public Iterator<E> iterator() {
        return new LazyListIterator();
    }

    public void setMaxResults(Integer maxResults) {
        // this.maxResults = maxResults;
    }

    private class LazyListIterator implements Iterator<E> {
        private int iNext = 0;

        public boolean hasNext() {
            loadAtleastItems(iNext);
            return backingList.size() > iNext;
        }

        public E next() {
            return get(iNext++);
        }

        public void remove() {
            LazyList.this.remove(iNext - 1);
        }
    }
}