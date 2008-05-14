package com.spaceprogram.simplejpa;

import com.spaceprogram.simplejpa.query.JPAQuery;
import com.spaceprogram.simplejpa.query.JPAQueryParser;
import com.spaceprogram.simplejpa.query.QueryImpl;
import com.spaceprogram.simplejpa.util.AmazonSimpleDBUtil;
import com.spaceprogram.simplejpa.util.ConcurrentRetriever;
import com.xerox.amazonws.sdb.*;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.LazyLoader;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

import javax.persistence.*;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 12:59:38 PM
 */
public class EntityManagerSimpleJPA implements SimpleEntityManager {

    private static Logger logger = Logger.getLogger(EntityManagerSimpleJPA.class.getName());
    private boolean closed = false;
    private EntityManagerFactoryImpl factory;
    /**
     * cache is used to store objects retrieved in this EntityManager session
     */
    private Map cache = new ConcurrentHashMap();
    /**
     * used for converting numbers to strings
     */
    public static final BigDecimal OFFSET_VALUE = new BigDecimal(Long.MIN_VALUE).negate();
    private int queryCount;
    private OpStats opStats = new OpStats();

    EntityManagerSimpleJPA(EntityManagerFactoryImpl factory) {
        this.factory = factory;
    }

    public void persist(Object o) {
        resetLastOpStats();
        try {
            new AsyncSaveTask(this, o).call();
        } catch (SDBException e) {
            throw new PersistenceException("Could not get SimpleDb Domain", e);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    private void resetLastOpStats() {
        opStats = new OpStats();
    }


    public Future persistAsync(Object o) {
        Future future = getExecutor().submit(new AsyncSaveTask(this, o));
        return future;
    }


    public <T> T merge(T t) {
        // todo: should probably behave a bit different
        persist(t);
        return t;
    }

    String lobKeyAttributeName(String columnName, Method getter) {
        return columnName != null ? columnName : attributeName(getter) + "-lobkey";
    }

    String s3ObjectId(String id, Method getter) {
        return id + "-" + attributeName(getter);
    }

    String s3bucketName() {
        return factory.getPersistenceUnitName() + "-lobs";
    }

    S3Service getS3Service() throws S3ServiceException {
        S3Service s3;
        AWSCredentials awsCredentials = new AWSCredentials((String) factory.getProps().get("accessKey"), (String) factory.getProps().get("secretKey"));
        s3 = new RestS3Service(awsCredentials);
        return s3;
    }

    String padOrConvertIfRequired(Object ob) {
        if (ob instanceof Integer || ob instanceof Long) {
            // then pad
            return AmazonSimpleDBUtil.encodeRealNumberRange(new BigDecimal(ob.toString()), AmazonSimpleDBUtil.LONG_DIGITS, OFFSET_VALUE);
        } else if (ob instanceof Double || ob instanceof Float) {
            // then pad
            return AmazonSimpleDBUtil.encodeRealNumberRange(new BigDecimal(ob.toString()), AmazonSimpleDBUtil.LONG_DIGITS, AmazonSimpleDBUtil.LONG_DIGITS,
                    OFFSET_VALUE);
        } else if (ob instanceof BigDecimal) {
            // then pad
            return AmazonSimpleDBUtil.encodeRealNumberRange((BigDecimal) ob, AmazonSimpleDBUtil.LONG_DIGITS, AmazonSimpleDBUtil.LONG_DIGITS,
                    OFFSET_VALUE);
        } else if (ob instanceof Date) {
            Date d = (Date) ob;
            return AmazonSimpleDBUtil.encodeDate(d);
        }
        return ob.toString();
    }


    /**
     * Get's the identifier for the object based on @Id
     *
     * @param o
     * @return
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    public String getId(Object o) {
        AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(o);
        if (ai == null) return null; // todo: should it throw?
        String id = null;
        try {
            id = (String) ai.getIdMethod().invoke(o);
        } catch (IllegalAccessException e) {
            throw new PersistenceException(e);
        } catch (InvocationTargetException e) {
            throw new PersistenceException(e);
        }
        return id;
    }

    public String getDomainName(Class<? extends Object> aClass) {
        return factory.getDomainName(aClass);
    }


    void checkEntity(Object o) {
        String className = o.getClass().getName();
        ensureClassIsEntity(className);
        // now if it the reflection data hasn't been cached, do it now
        AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(o);
        String domainName = getDomainName(o.getClass());
        factory.setupDbDomain(domainName);
    }

    public Class ensureClassIsEntity(String className) {
        className = factory.getAnnotationManager().stripEnhancerClass(className);
        String fullClassName = factory.getEntityMap().get(className);
        if (fullClassName == null) {
            throw new PersistenceException("Object not marked as an Entity: " + className);
        }
        Class tClass = factory.getAnnotationManager().getClass(fullClassName);
        AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(tClass); // sets up metadata if not already done
        return tClass;
    }


    public SimpleDB getSimpleDb() {
        return factory.getSimpleDb();
    }

    /**
     * Deletes an object from SimpleDB.
     *
     * @param o
     */
    public void remove(Object o) {
        try {
            Domain domain = getDomain(o.getClass());
            String id = getId(o);
            logger.fine("deleting item with id: " + id);
            domain.deleteItem(id);
            cacheRemove(cacheKey(o.getClass(), id));
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    /**
     * Finds an object by id.
     *
     * @param tClass
     * @param id
     * @return
     */
    public <T> T find(Class<T> tClass, Object id) {
        if (closed) throw new PersistenceException("EntityManager already closed.");
        if (id == null) throw new IllegalArgumentException("Id value must not be null.");
        try {
            T ob = (T) cacheGet(cacheKey(tClass, id));
            if (ob != null) {
                logger.fine("found in cache: " + ob);
                return ob;
            }
            Domain domain = getDomain(tClass);
            Item item = domain.getItem(id.toString());
//            logger.fine("got back item=" + item);
            if (item == null) return null;
            List<ItemAttribute> atts = item.getAttributes();
            if (atts == null || atts.size() == 0) return null;
            return buildObject(tClass, id, atts);
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
    }

    public void rename(Class tClass, String oldAttributeName, String newAttributeName) {
        // get list of all items in the domain
        try {
            Domain domain = getDomain(tClass);
            QueryResult result;
            List<Item> items;
            int i = 0;
            String nextToken = null;
            while (i == 0 || nextToken != null) {
                result = executeQueryForRename(oldAttributeName, newAttributeName, domain, nextToken);
                items = result.getItemList();
                putAndDelete(oldAttributeName, newAttributeName, items);
                nextToken = result.getNextToken();
                i++;
            }
        } catch (SDBException e) {
            e.printStackTrace();
        }
    }

    private QueryResult executeQueryForRename(String oldAttributeName, String newAttributeName, Domain domain, String nextToken) throws SDBException {
        QueryResult result = domain.listItems("['" + oldAttributeName + "' starts-with ''] intersection not ['" + newAttributeName + "' starts-with ''] ", nextToken, 100);
        return result;
    }

    private void putAndDelete(String oldAttributeName, String newAttributeName, List<Item> items) throws SDBException {
        for (Item item : items) {
            List<ItemAttribute> oldAtts = item.getAttributes(oldAttributeName);
            if (oldAtts.size() > 0) {
                ItemAttribute oldAtt = oldAtts.get(0);
                List<ItemAttribute> atts = new ArrayList<ItemAttribute>();
                atts.add(new ItemAttribute(newAttributeName, oldAtt.getValue(), true));
                item.putAttributes(atts);
                item.deleteAttributes(oldAtts);
            }
        }
    }

    /**
     * Gets the Typica Domain for a class.
     *
     * @param c
     * @return
     * @throws SDBException
     */
    public <T> Domain getDomain(Class<T> c) throws SDBException {
        c = factory.getAnnotationManager().stripEnhancerClass(c);
        return factory.getDomain(c);
    }

    /**
     * This method puts together an object from the SimpleDB data.
     *
     * @param tClass
     * @param id
     * @param atts
     * @return
     */
    public <T> T buildObject(Class<T> tClass, Object id, List<ItemAttribute> atts) {
        return ObjectBuilder.buildObject(this, tClass, id, atts);
    }


    public Object cacheGet(String key) {
        logger.fine("getting item from cache with cachekey=" + key);
        Object o = cache.get(key);
        logger.fine("got item from cache=" + o);
        return o;
    }

    public void cachePut(String key, Object newInstance) {
        logger.fine("putting item in cache with cachekey=" + key + " - " + newInstance);
        cache.put(key, newInstance);
    }

    private Object cacheRemove(String key) {
        logger.fine("removing item from cache with cachekey=" + key);
        Object o = cache.remove(key);
        logger.fine("removed object from cache=" + o);
        return o;
    }

    public String cacheKey(Class tClass, Object id) {
        return factory.getAnnotationManager().stripEnhancerClass(tClass).getName() + "_" + id;
    }


    public String foreignKey(Method getter) {
        return foreignKey(attributeName(getter));
    }

    public String foreignKey(String attName) {
        return attName + "_id";
    }

    private Object newLazyLoadingInstance(final Class retType, final Object id) {
        return Enhancer.create(retType,
                new LazyLoader() {
                    public Object loadObject() {
                        try {
                            logger.fine("loadObject called for type=" + retType + " with id=" + id);
                            return find(retType, id);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
    }

    public Method getSetterFromGetter(Class tClass, Method getter, Class retType) throws NoSuchMethodException {
        return tClass.getMethod(getSetterNameFromGetter(getter), retType);
    }

    String getSetterNameFromGetter(Method getter) {
        return setterName(attributeName(getter));
    }

    String getGetterNameFromSetter(Method setter) {
        return getterName(attributeName(setter));
    }

    private String setterName(String fieldName) {
        return "set" + StringUtils.capitalize(fieldName);
    }

    public String attributeName(Method getterOrSetter) {
        return StringUtils.uncapitalize(getterOrSetter.getName().substring(3));
    }

    static <T> String getterName(String fieldName) {
        return ("get" + StringUtils.capitalize(fieldName));
    }

    /**
     * Sets the value on an object field after applying any necessary conversions from SimpleDB strings.
     *
     * @param tClass
     * @param newInstance
     * @param getter
     * @param val
     */
    <T> void setFieldValue(Class tClass, T newInstance, Method getter, String val) {
        try {
            // need param type
            String attName = attributeName(getter);
            Class retType = getter.getReturnType();
//            logger.fine("getter in setFieldValue = " + attName + " - val=" + val + " rettype=" + retType);
            Method setMethod = tClass.getMethod("set" + StringUtils.capitalize(attName), retType);
            if (Integer.class.isAssignableFrom(retType)) {
//                logger.fine("setting int val " + val + " on field " + attName);
                val = AmazonSimpleDBUtil.decodeRealNumberRange(val, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
            } else if (Long.class.isAssignableFrom(retType)) {
                val = AmazonSimpleDBUtil.decodeRealNumberRange(val, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
            } else if (Double.class.isAssignableFrom(retType)) {
                val = AmazonSimpleDBUtil.decodeRealNumberRange(val, AmazonSimpleDBUtil.LONG_DIGITS, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
            } else if (BigDecimal.class.isAssignableFrom(retType)) {
                val = AmazonSimpleDBUtil.decodeRealNumberRange(val, AmazonSimpleDBUtil.LONG_DIGITS, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
            } else if (Date.class.isAssignableFrom(retType)) {
                try {
                    setMethod.invoke(newInstance, AmazonSimpleDBUtil.decodeDate(val));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                val = null;
            }
            if (val != null) {
                // todo: getConstructor throws a NoSuchMethodException here, should ensure that these are second class object fields
                Constructor forNewField = retType.getConstructor(val.getClass());
                if (forNewField == null) {
                    throw new PersistenceException("No constructor for field type: " + retType + " that can take a " + val.getClass());
                }
                Object newField = forNewField.newInstance(val);
                setMethod.invoke(newInstance, newField);
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

    public <T> T getReference(Class<T> tClass, Object o) {
        throw new NotImplementedException("TODO");
    }

    public void flush() {
        throw new NotImplementedException("TODO");
    }

    public void setFlushMode(FlushModeType flushModeType) {
        throw new NotImplementedException("TODO");
    }

    public FlushModeType getFlushMode() {
        throw new NotImplementedException("TODO");
    }

    public void lock(Object o, LockModeType lockModeType) {
        throw new NotImplementedException("TODO");
    }

    public void refresh(Object o) {
        throw new NotImplementedException("TODO");
    }

    public void clear() {
        throw new NotImplementedException("TODO");
    }

    public boolean contains(Object o) {
        throw new NotImplementedException("TODO");
    }

    public Query createQuery(String s) {
        logger.fine("query=" + s);
        JPAQuery q = new JPAQuery();
        JPAQueryParser parser = new JPAQueryParser(q, s);
        parser.parse();
        String from = q.getFrom();
        logger.finer("from=" + from);
        logger.finer("where=" + q.getFilter());
        if (q.getOrdering() != null) {
            throw new UnsupportedOperationException("ORDER BY not supported.");
        }
        return new QueryImpl(this, q);
    }

    public Query createNamedQuery(String s) {
        throw new NotImplementedException("TODO");
    }

    public Query createNativeQuery(String s) {
        throw new NotImplementedException("TODO");
    }

    public Query createNativeQuery(String s, Class aClass) {
        throw new NotImplementedException("TODO");
    }

    public Query createNativeQuery(String s, String s1) {
        throw new NotImplementedException("TODO");
    }

    public void joinTransaction() {
        throw new NotImplementedException("TODO");
    }

    public Object getDelegate() {
        throw new NotImplementedException("TODO");
    }

    /**
     * Clears cache and marks as closed.
     */
    public void close() {
        closed = true;
        cache = null;
    }

    public boolean isOpen() {
        return !closed;
    }

    public EntityTransaction getTransaction() {
        return new EntityTransactionImpl();
    }

    public ExecutorService getExecutor() {
        return factory.getExecutor();
    }

    public Object getObjectFromS3(String idOnS3) throws S3ServiceException, IOException, ClassNotFoundException {
        S3Service s3 = getS3Service();
        S3Bucket bucket = s3.createBucket(s3bucketName());
        S3Object s3o = s3.getObject(bucket, idOnS3);
        logger.fine("got s3object=" + s3o);
        if (s3o != null) {
            ObjectInputStream reader = new ObjectInputStream(new BufferedInputStream((s3o.getDataInputStream())));
            Object o = reader.readObject();
            s3o.closeDataInputStream();
            return o;
        }
        return null;
    }

    public void incrementQueryCount() {
        queryCount++;
    }

    /**
     * @return the number of actual queries sent to amazon.
     */
    public int getQueryCount() {
        return queryCount;
    }

    public void setQueryCount(int queryCount) {
        this.queryCount = queryCount;
    }

    /**
     * This is mainly for debugging purposes. Will print to system out all of the items in the domain represented
     * by the class parameter.
     *
     * @param c
     * @throws SDBException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public void listAllObjectsRaw(Class c) throws SDBException, ExecutionException, InterruptedException {
        Domain d = getDomain(c);
        QueryResult qr = d.listItems();
        List<ItemAndAttributes> ia = ConcurrentRetriever.getAttributesFromSdb(qr.getItemList(), getExecutor());
        for (ItemAndAttributes itemAndAttributes : ia) {
            System.out.println("item=" + itemAndAttributes.getItem().getIdentifier());
            List<ItemAttribute> atts = itemAndAttributes.getAtts();
            for (ItemAttribute att : atts) {
                System.out.println("\t=" + att.getName() + "=" + att.getValue());
            }
        }
    }

    public AnnotationManager getAnnotationManager() {
        return factory.getAnnotationManager();
    }


    public EntityManagerFactoryImpl getFactory() {
        return factory;
    }


    public OpStats getOpStats() {
        return opStats;
    }
}
