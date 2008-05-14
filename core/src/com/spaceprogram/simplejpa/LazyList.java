package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.*;
import org.apache.commons.collections.list.GrowthList;

import javax.persistence.PersistenceException;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Loads objects in the list on demand from SimpleDB.
 * <p/>
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 9:06:16 PM
 */
public class LazyList extends AbstractList implements Serializable {
    private static Logger logger = Logger.getLogger(LazyList.class.getName());
    private EntityManagerSimpleJPA em;
//    private Object instance;
    //    private String fieldName;
    //    private Object id;
    private Class genericReturnType;
    private String query;
    private AnnotationInfo ai;
    //    private boolean loadedItems;
    private List<Item> items = new ArrayList<Item>();
    //    private int size;
    private List backingList = new GrowthList();
    private int numRetrieved = 0;
    private String nextToken;
    private int maxResults = 100; // same as amazon default
    private int numPagesLoaded;

    public LazyList() {
        super();
    }

    public LazyList(EntityManagerSimpleJPA em, Class tClass, String query) {
        this();
        this.em = em;
//        this.instance = instance;
//        this.fieldName = fieldName;
//        this.id = id;
        this.genericReturnType = tClass;
        this.query = query;
        this.ai = em.getFactory().getAnnotationManager().getAnnotationInfo(tClass);
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
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
        loadItems(i / maxResults);
        logger.fine("getting from lazy list at index=" + i);
        Object o;
        if (backingList.size() > i) {
            o = backingList.get(i);
            if (o != null) {
                logger.fine("object already loaded: " + o);
                return o;
            }
        }
        // else we load the atts
        Item item = items.get(i);
        o = em.cacheGet(em.cacheKey(genericReturnType, item.getIdentifier()));
        if (o != null) {
            logger.fine("cache hit in lazy list");
            return o;
        }
        try {
            List<ItemAttribute> atts = item.getAttributes();
            o = em.buildObject(genericReturnType, item.getIdentifier(), atts);
            backingList.set(i, o);
            return o;
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
    }

    private synchronized void loadItems(int page) {
        if (numPagesLoaded > page) return;
        if (numPagesLoaded > 0 && nextToken == null) {
            // no more results to get
            return;
        }

//        genericReturnType.getOwnerType()
        Domain domain;
        try {
            domain = em.getDomain(ai.getRootClass());
            if(domain == null){
                logger.warning("Domain does not exist for " + ai.getRootClass());
                numPagesLoaded = 1;
                return;
            }
            // Now we'll load up all the items up to the page specified
            for (int i = numPagesLoaded; i <= page; i++) {
                logger.fine("loading items for list. Page=" + i);
                if (numPagesLoaded > 0 && nextToken == null) {
                    break;
                }
                QueryResult qr;
                try {
                    logger.fine("query for lazylist=" + query);
                    qr = domain.listItems(query, nextToken, maxResults);
                    logger.fine("got items for lazylist=" + qr.getItemList().size());
                    items.addAll(qr.getItemList());
                    nextToken = qr.getNextToken();
                    numRetrieved += qr.getItemList().size();
                    numPagesLoaded++;
                } catch (SDBException e) {
                    if (e.getMessage() != null && e.getMessage().contains("The specified domain does not exist")) {
                        items = new ArrayList<Item>(); // no need to throw here
                    } else {
                        throw new PersistenceException(e);
                    }
                }
                logger.fine("got " + items.size() + " for lazy list");
                // todo: we really need the size of the full list here. Or load up all the ids here via multiple calls with next token
                //            size = items.size();
                //            loadedItems = true;
            }
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
    }


    private void loadAllItems() {
        int page = 0;
        while (nextToken != null || numPagesLoaded == 0) {
            loadItems(page);
            page++;
        }
    }
}
