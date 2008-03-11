package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.*;
import org.apache.commons.collections.list.GrowthList;

import javax.persistence.PersistenceException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Loads objects in the list on demand from SimpleDB.
 *
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 9:06:16 PM
 */
public class LazyList extends AbstractList {
    private static Logger logger = Logger.getLogger(LazyList.class.getName());
    private EntityManagerSimpleJPA em;
    private Object instance;
    private String fieldName;
    private Object id;
    private Class genericReturnType;
    private AnnotationInfo ai;
    private boolean loadedItems;
    private List<Item> items;
    private int size;
    private List backingList = new GrowthList();

    public LazyList() {
        super();
    }

    public LazyList(EntityManagerSimpleJPA em, Object instance, String fieldName, Object id, Class genericReturnType, AnnotationInfo ai) {
        this();
        this.em = em;
        this.instance = instance;
        this.fieldName = fieldName;
        this.id = id;
        this.genericReturnType = genericReturnType;
        this.ai = ai;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public int size() {
        loadItems();
        return size;
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

    public Object get(int i) {
        loadItems();
        logger.finer("getting from lazy list at index=" + i);
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



    private synchronized void loadItems() {
        if (loadedItems) return;
        logger.fine("loading items for list.");
//        genericReturnType.getOwnerType()
        Domain domain = null;
        try {
            domain = em.getDomain(ai.getRootClass());
            String query = "['" + em.foreignKey(fieldName) + "' = '" + id + "']";
            if (ai.getDiscriminatorValue() != null) {
                query += " intersection ['DTYPE' = '" + ai.getDiscriminatorValue() + "']";
            }
            logger.fine("query=" + query);
            QueryResult qr;
            try {
                qr = domain.listItems(query);
                items = qr.getItemList();
            } catch (SDBException e) {
                if (e.getMessage() != null && e.getMessage().contains("The specified domain does not exist")) {
                    items = new ArrayList<Item>(); // no need to throw here
                } else {
                    throw new PersistenceException(e);
                }
            }
            logger.fine("got " + items.size() + " for lazy list");
            // todo: we really need the size of the full list here. Or load up all the ids here via multiple calls with next token
            size = items.size();
            loadedItems = true;
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
    }
}
