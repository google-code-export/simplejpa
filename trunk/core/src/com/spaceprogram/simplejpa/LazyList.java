package com.spaceprogram.simplejpa;

import com.spaceprogram.simplejpa.operations.GetAttributes;
import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.ItemAttribute;
import com.xerox.amazonws.sdb.QueryWithAttributesResult;
import com.xerox.amazonws.sdb.SDBException;
import org.apache.commons.collections.list.GrowthList;

import javax.persistence.PersistenceException;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Loads objects in the list on demand from SimpleDB.
 * <p/>
 *
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 9:06:16 PM
 */
public class LazyList extends AbstractList implements Serializable {
    private static Logger logger = Logger.getLogger(LazyList.class.getName());
    private transient EntityManagerSimpleJPA em;
    private Class genericReturnType;
    private String query;
    private List<SdbItem> items = new ArrayList<SdbItem>();
    /** Stores the actual objects for this list */
    private List backingList = new GrowthList();
    private int numRetrieved = 0;
    private String nextToken;
    private int maxToRetrievePerRequest = 100; // same as amazon default
    private int numPagesLoaded;
    private transient Map<String, Future<ItemAndAttributes>> futuresMap = new ConcurrentHashMap();
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

    public LazyList(EntityManagerSimpleJPA em, Class tClass, String query) {
        this();
        this.em = em;
        this.genericReturnType = tClass;
        this.query = query;

    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
//        System.out.println("size called");
        loadAllItems();
        return numRetrieved;
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
        Object o;
        if (backingList.size() > i) {
            o = backingList.get(i);
            if (o != null) {
                /*if (o instanceof Future) {
                    Future future = (Future) o;

                }*/
                logger.finest("object already loaded in backing list: " + o);
                return o;
            }
        }
        // else we load the atts
        SdbItem item = items.get(i);
        // check future's loading map
        try {
            o = checkFuturesMap(item);
            if (o == null) {
                o = checkCache(item); // todo: should this be swapped with checkFuturesMap and can cancel the future if for some reason it got cached between the future being added and here
                if (o == null) {
                    // get from DB
                    o = em.getItemAttributesBuildAndCache(genericReturnType, item.getIdentifier(), item);

                } else {
                    if (logger.isLoggable(Level.FINEST)) logger.finest("cache hit in lazy list: " + o);
                }
                if(o == null){
                    // can this ever happen??  perhaps if the item gets deleted somewhere along the way?
                    throw new PersistenceException("SHOULD NEVER GET THIS EXCEPTION. getting item at position " + i + ", list.size=" + size());
                }
            }
            putInBackingList(i, o);
            return o;
        } catch (PersistenceException e) {
            throw e;
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    private Object checkFuturesMap(SdbItem item) throws ExecutionException, InterruptedException {
        Future<ItemAndAttributes> f = futuresMap.remove(item.getIdentifier());
        if (f != null) {
//            System.out.println("getting object from futures map...");
            ItemAndAttributes ia = f.get();
            Object o = em.buildObject(genericReturnType, ia.getItem().getIdentifier(), ia.getAtts());
            return o;
        }
        return null;
    }

    private Object checkCache(SdbItem item) {
        Object o = em.cacheGet(genericReturnType, item.getIdentifier());
        return o;
    }

    private void putInBackingList(int i, Object o) {
        backingList.set(i, o);
    }

    private synchronized void loadItems(int page, boolean materializeObjectsInPage) {
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
                    qr = domain.selectItems(query, nextToken); // todo: maxToRetrievePerRequest, need to use limit now
                    Map<String, List<ItemAttribute>> itemMap = qr.getItems();
//                    List<Item> itemList = qr.getResultList();
                    if (logger.isLoggable(Level.FINER)) logger.finer("got items for lazylist=" + itemMap.size());
                    // dang, this new select thing in typica sucks, why would the list of results be a map?
                    for (String id : itemMap.keySet()) {
                        List<ItemAttribute> list = itemMap.get(id);
                        items.add(new SdbItemImpl(id, list));
                    }

//                    items.addAll(itemList);
                    numRetrieved += itemMap.size();
                    nextToken = qr.getNextToken();
                    numPagesLoaded++;
                    if (materializeObjectsInPage) {
                        materializeObjectsInPage(page);
                    }
                } catch (SDBException e) {
                    if (ExceptionHelper.isDomainDoesNotExist(e)) {
                        items = new ArrayList<SdbItem>(); // no need to throw here
                    } else {
                        throw new PersistenceException("Query failed: Domain=" + domain.getName() + " -> " + query, e);
                    }
                }
                logger.finer("got " + items.size() + " for lazy list");
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
        int start = page * maxToRetrievePerRequest;
        int end = start + maxToRetrievePerRequest;
        if (end > items.size()) end = items.size();
        List<SdbItem> itemList = this.items.subList(start, end);
        if (itemList.size() != 0) {
            // todo: could send this off asyncronously and only block when asking for a particular item. This is done now.
            try {
                // check cache first to make sure we haven't already got these
                List<SdbItem> itemsToGet = new ArrayList<SdbItem>();
                for (SdbItem item : itemList) {
                    Future<ItemAndAttributes> f = futuresMap.get(item.getIdentifier());
                    if (f == null) {
                        Object o = checkCache(item);
                        if (o == null) {
                            // if it's already cached, no need to retrieve it again.
                            itemsToGet.add(item);
                        } else {
                            //                        System.out.println("found item in cache while materializing. All good.");
                        }
                    }
                }

                for (SdbItem item : itemsToGet) {
                    // todo: Make this async do the buildObject call so it gets in the cache as soon as possible
                    Callable<ItemAndAttributes> callable = new GetAttributes(item, em);
                    Future<ItemAndAttributes> itemAndAttributesFuture = em.getExecutor().submit(callable);
                    // todo:  em.statsGets(attributes.size(), duration2); - put this into the Callable
                    futuresMap.put(item.getIdentifier(), itemAndAttributesFuture);
                }

                /* if(logger.isLoggable(Level.FINER)) logger.finer("Loading " + itemList.size() + " asynchronously.");
                long start2 = System.currentTimeMillis();
                List<ItemAndAttributes> attributes = ConcurrentRetriever.getAttributesFromSdb(itemsToGet, em.getExecutor());
                long duration2 = System.currentTimeMillis() - start2;
                if(logger.isLoggable(Level.FINE))logger.fine("getAtts time=" + (duration2));
                em.statsGets(attributes.size(), duration2);
                for (ItemAndAttributes ia : attributes) {
                    Object o = em.buildObject(genericReturnType, ia.getItem().getIdentifier(), ia.getAtts());
                    // now it will be in the cache too, so next call get get() on this list will first get it from the cache
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
