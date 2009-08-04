package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.ItemAttribute;
import com.xerox.amazonws.sdb.QueryWithAttributesResult;
import com.xerox.amazonws.sdb.SDBException;
import com.spaceprogram.simplejpa.query.QueryImpl;
import com.spaceprogram.simplejpa.query.JPAQuery;
import org.apache.commons.collections.list.GrowthList;
import org.junit.Assert;

import javax.persistence.PersistenceException;
import javax.persistence.Query;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads objects in the list on demand from SimpleDB.
 * <p/>
 * <p/>
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 9:06:16 PM
 */
public class LazyList extends AbstractList implements Serializable {
    private static Logger logger = Logger.getLogger(LazyList.class.getName());
    private transient EntityManagerSimpleJPA em;
    private Class genericReturnType;
    private QueryImpl query;
    //    private List<Object> items = new ArrayList();
    /**
     * Stores the actual objects for this list
     */
    private List backingList = new GrowthList();
    //    private int numRetrieved = 0;
    private String nextToken;
    private int maxToRetrievePerRequest = 100; // same as amazon default
    private int numPagesLoaded;
    private Long count;
    //    private transient Map<String, Future<ItemAndAttributes>> futuresMap = new ConcurrentHashMap();
    /**
     * map to remember which pages have been loaded already.
     */
    private Map<Integer, Integer> materializedPages = new HashMap<Integer, Integer>();
    /**
     * Max results based on the query sent in.
     * todo: implement using this
     */
    private Integer maxResults;

    public LazyList() {
        super();
    }

    public LazyList(EntityManagerSimpleJPA em, Class tClass, QueryImpl query) {
        this();
        this.em = em;
        this.genericReturnType = tClass;
        this.query = query;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        // todo: do a quick precheck if no next token or something, then no need to call this count
        if (count != null) return count.intValue();
        try {
            JPAQuery queryClone = (JPAQuery) query.getQ().clone();
            queryClone.setResult("count(*)");
            Query query = new QueryImpl(em, queryClone);
            List results = query.getResultList();
            System.out.println("obs.size=" + results.size());
            count = (Long) results.get(0);
            System.out.println("count set to " + count);

        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
//        loadAllItems();
        return count.intValue();
    }

    public void add(int index, Object element) {
        backingList.add(index, element);
    }

    public Object set(int index, Object element) {
        return backingList.set(index, element);
    }

    public Object remove(int index) {
        return backingList.remove(index);
    }

    public static void main(String[] args) {
        int x = 32 / 100;
        System.out.println("x=" + x);
    }

    public Object get(int i) {
        if (logger.isLoggable(Level.FINER)) logger.finer("getting from lazy list at index=" + i);
        loadItems(i / maxToRetrievePerRequest, true);
        Object o = null;
        if (backingList.size() > i) {
            o = backingList.get(i);
            if (o != null) {
                logger.finest("object already loaded in backing list: " + o);
                return o;
            }
        }
        return o;
    }

    private Object checkCache(SdbItem item) {
        Object o = em.cacheGet(genericReturnType, item.getIdentifier());
        return o;
    }

    private void putInBackingList(int i, Object o) {
        backingList.set(i, o);
    }

    private synchronized void loadItems(int page, boolean materializeObjectsInPage) {
        System.out.println("loadItems page=" + page);
        if (numPagesLoaded > page) {
            if (materializeObjectsInPage) materializeObjectsInPage(page);
            return;
        }
        if (numPagesLoaded > 0 && nextToken == null) {
            // no more results to get
            if (materializeObjectsInPage) materializeObjectsInPage(page);
            return;
        }

        Domain domain;
        try {
            AnnotationInfo ai = em.getAnnotationManager().getAnnotationInfo(genericReturnType);
            domain = em.getDomain(ai.getRootClass());
            if (domain == null) {
                logger.warning("Domain does not exist for " + ai.getRootClass());
                numPagesLoaded = 1;
                return;
            }
            // Now we'll load up all the items up to the page specified
            for (int i = numPagesLoaded; i <= page; i++) {
                if (logger.isLoggable(Level.FINER)) logger.finer("loading items for list. Page=" + i);
                if (numPagesLoaded > 0 && nextToken == null) {
                    break;
                }
                QueryWithAttributesResult qr;
                try {
                    if (logger.isLoggable(Level.FINER)) logger.finer("query for lazylist=" + query);
                    if (em.getFactory().isPrintQueries()) System.out.println("query in lazylist=" + query);
                    qr = domain.selectItems(query.getAmazonQuery().getValue(), nextToken); // todo: maxToRetrievePerRequest, need to use limit now
                    Map<String, List<ItemAttribute>> itemMap = qr.getItems();
//                    List<Item> itemList = qr.getResultList();
                    if (logger.isLoggable(Level.FINER)) logger.finer("got items for lazylist=" + itemMap.size());
                    // dang, this new select thing in typica sucks, why would the list of results be a map?
                    for (String id : itemMap.keySet()) {
                        List<ItemAttribute> list = itemMap.get(id);
                        backingList.add(em.buildObject(genericReturnType, id, list));
                    }

//                    items.addAll(itemList);
//                    numRetrieved += itemMap.size();
                    nextToken = qr.getNextToken();
                    numPagesLoaded++;
                    if (materializeObjectsInPage) {
                        materializeObjectsInPage(page);
                    }
                } catch (SDBException e) {
                    if (ExceptionHelper.isDomainDoesNotExist(e)) {
//                        items = new ArrayList(); // no need to throw here
                    } else {
                        throw new PersistenceException("Query failed: Domain=" + domain.getName() + " -> " + query, e);
                    }
                }
//                logger.finer("got " + items.size() + " for lazy list");
            }
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
    }

    private void materializeObjectsInPage(int page) {
        if (materializedPages.get(page) != null) {
            return;
        }
        materializedPages.put(page, page);

    }


    private void loadAllItems() {
//        System.out.println("loadAllItems");
        int page = 0;
        while (nextToken != null || numPagesLoaded == 0) {
            loadItems(page, false);
            page++;
        }
    }


    public void setMaxResults(Integer maxResults) {
        this.maxResults = maxResults;
    }

}
