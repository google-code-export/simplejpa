package com.spaceprogram.simplejpa;

import com.spaceprogram.simplejpa.util.AmazonSimpleDBUtil;
import com.spaceprogram.simplejpa.query.JPAQuery;
import com.spaceprogram.simplejpa.query.JPAQueryParser;
import com.spaceprogram.simplejpa.query.QueryImpl;
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
import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 12:59:38 PM
 */
public class EntityManagerSimpleJPA implements EntityManager {

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

    EntityManagerSimpleJPA(EntityManagerFactoryImpl factory) {
        this.factory = factory;
    }

    public void persist(Object o) {
        checkEntity(o);
//        SimpleDB db = getSimpleDb();
        try {
            AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(o);
            Domain domain;
            if (ai.getRootClass() != null) {
                domain = getDomain(ai.getRootClass());
            } else {
                domain = getDomain(o.getClass());
            }
            // create id if required
            String id = getId(o);
            boolean newOb = false;
            if (id == null) {
                id = UUID.randomUUID().toString();
                newOb = true;
            }
            setFieldValue(o.getClass(), o, ai.getIdMethod(), id);
            Item item = domain.getItem(id);
            // now set attributes
            List<ItemAttribute> atts = new ArrayList<ItemAttribute>();
            if (ai.getDiscriminatorValue() != null) {
                atts.add(new ItemAttribute("DTYPE", ai.getDiscriminatorValue(), true));
            }
            Collection<Method> getters = ai.getGetters();
            List<ItemAttribute> attsToDelete = new ArrayList<ItemAttribute>();
            for (Method getter : getters) {
                Object ob = getter.invoke(o);
                String attName = attributeName(getter);
                if (ob == null) {
                    attsToDelete.add(new ItemAttribute(attName, null, true));
                    // todo: what about lobs?  need to delete from s3
                    continue;
                }
                if (getter.getAnnotation(ManyToOne.class) != null) {
                    // store the id of this object
                    String id2 = getId(ob);
                    atts.add(new ItemAttribute(foreignKey((Method) getter), id2, true));
                } else if (getter.getAnnotation(OneToMany.class) != null) {
                    // FORCING BI-DIRECTIONAL RIGHT NOW SO JUST IGNORE
                } else if (getter.getAnnotation(Lob.class) != null) {
                    // store in s3
                    S3Service s3 = null;
                    try {
                        // todo: need to make sure we only store to S3 if it's changed, too slow.
                        logger.fine("putting lob to s3");
                        s3 = getS3Service();
                        S3Bucket bucket = s3.createBucket(s3bucketName()); // todo: only do this once per EMFactory
                        String s3ObjectId = s3ObjectId(id, getter);
                        S3Object s3Object = new S3Object(bucket, s3ObjectId);
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        ObjectOutputStream out;
                        out = new ObjectOutputStream(bos);
                        out.writeObject(ob);
                        s3Object.setDataInputStream(new ByteArrayInputStream(bos.toByteArray()));
                        s3Object = s3.putObject(bucket, s3Object);
                        out.close();
                        logger.fine("setting lobkeyattribute=" + lobKeyAttributeName(getter) + " - " + s3ObjectId);
                        atts.add(new ItemAttribute(lobKeyAttributeName(getter), s3ObjectId, true));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    String toSet = ob != null ? padOrConvertIfRequired(ob) : "";
                    // todo: throw an exception if this is going to exceed maximum size, suggest using @Lob
                    atts.add(new ItemAttribute(attributeName(getter), toSet, true));
                }
            }
            // and now finally send it for storage
            item.putAttributes(atts);
            if (attsToDelete.size() > 0) {
                item.deleteAttributes(attsToDelete);
            }
        } catch (SDBException e) {
            throw new PersistenceException("Could not get SimpleDb Domain", e);
        } catch (InvocationTargetException e) {
            throw new PersistenceException(e);
        } catch (IllegalAccessException e) {
            throw new PersistenceException(e);
        }
    }


    public <T> T merge(T t) {
        // todo: should probably behave a bit different
        persist(t);
        return t;
    }

    String lobKeyAttributeName(Method getter) {
        return attributeName(getter) + "-lobkey";
    }

    private String s3ObjectId(String id, Method getter) {
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

    private String padOrConvertIfRequired(Object ob) {
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
        String className = getRootClassName(aClass);
        return factory.getPersistenceUnitName() + "-" + className;
    }

    private String getRootClassName(Class<? extends Object> aClass) {
        AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(aClass);
        String className = ai.getRootClass().getSimpleName();
        return className;
    }

    private void checkEntity(Object o) {
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

    /**
     * Gets the Typica Domain for a class.
     *
     * @param c
     * @return
     * @throws SDBException
     */
    public <T> Domain getDomain(Class<T> c) throws SDBException {
        c = factory.getAnnotationManager().stripEnhancerClass(c);
        String domainName = getDomainName(c);
        return getDomain(domainName);
    }

    /**
     * Gets the typica domain for a domain name.
     *
     * @param domainName
     * @return
     * @throws SDBException
     */
    public Domain getDomain(String domainName) throws SDBException {
        SimpleDB db = getSimpleDb();
        Domain domain = db.getDomain(domainName);
        return domain;
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
        T newInstance = (T) cacheGet(cacheKey(tClass, id));
        if (newInstance != null) return newInstance;
        AnnotationInfo ai = factory.getAnnotationManager().getAnnotationInfo(tClass);
        try {
//            newInstance = tClass.newInstance();
            // check for DTYPE to see if it's a subclass, must be a faster way to do this you'd think?
            for (ItemAttribute att : atts) {
                if(att.getName().equals("DTYPE")){
                    System.out.println("dtype=" + att.getValue());
                    ai = factory.getAnnotationManager().getAnnotationInfoByDiscriminator(att.getValue());
                    tClass = ai.getMainClass();
                    // check cache again with new class
                    newInstance = (T) cacheGet(cacheKey(tClass, id));
                    if (newInstance != null) return newInstance;
                    break;
                }
            }
            ObjectWithInterceptor owi = newEnancedInstance(tClass);
            newInstance = (T) owi.getBean();
            Collection<Method> getters = ai.getGetters();
            for (Method getter : getters) {
                String attName = attributeName(getter);
                if (getter.getAnnotation(ManyToOne.class) != null) {
                    // lazy it up
                    String identifierForManyToOne = getIdentifierForManyToOne(getter, atts);
                    logger.fine("identifierForManyToOne=" + identifierForManyToOne);
                    if (identifierForManyToOne == null) {
                        continue;
                    }
                    // todo: stick a cache in here and check the cache for the instance before creating the lazy loader.
                    logger.finer("creating new lazy loading instance for getter " + getter.getName() + " of class " + tClass.getSimpleName() + " with id " + id);
//                    Object toSet = newLazyLoadingInstance(retType, identifierForManyToOne);
                    owi.getInterceptor().putForeignKey(attName, identifierForManyToOne);
                } else if (getter.getAnnotation(OneToMany.class) != null) {
                    OneToMany annotation = getter.getAnnotation(OneToMany.class);
                    ParameterizedType type = (ParameterizedType) getter.getGenericReturnType();
//                    logger.fine("type for manytoone=" + type + " " + type.getClass().getName()  + " " + type.getRawType() + " " + type.getOwnerType());
                    Type[] types = type.getActualTypeArguments();
                    Class typeInList = (Class) types[0];
                    // todo: should this return null if there are no elements??
                    LazyList lazyList = new LazyList(this, newInstance, annotation.mappedBy(), id, typeInList, factory.getAnnotationManager().getAnnotationInfo(typeInList));
                    Class retType = getter.getReturnType();
                    // todo: assuming List for now, handle other collection types
                    String setterName = getSetterFromGetter(getter);
                    Method setter = tClass.getMethod(setterName, retType);
                    setter.invoke(newInstance, lazyList);
                } else if (getter.getAnnotation(Lob.class) != null) {
                    // handled in Proxy
                    String lobKeyVal = getValueToSet(atts, lobKeyAttributeName(getter));
                    logger.fine("lobkeyval to set on interceptor=" + lobKeyVal + " - fromatt=" + lobKeyAttributeName(getter));
                    if (lobKeyVal != null) owi.getInterceptor().putForeignKey(attName, lobKeyVal);
                } else {
                    String val = getValueToSet(atts, attName);
                    if (val != null) {
                        setFieldValue(tClass, newInstance, getter, val);
                    }
                }
            }
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
        cachePut(cacheKey(tClass, id), newInstance);
        return newInstance;
    }

    private String getValueToSet(List<ItemAttribute> atts, String propertyName) {
        for (ItemAttribute att : atts) {
            String attName = att.getName();
            if (attName.equals(propertyName)) {
                String val = att.getValue();
                return val;
            }
        }
        return null;
    }

    private ObjectWithInterceptor newEnancedInstance(Class tClass) {
        LazyInterceptor interceptor = new LazyInterceptor(this);
        Enhancer e = new Enhancer();
        e.setSuperclass(tClass);
        e.setCallback(interceptor);
        Object bean = e.create();
        ObjectWithInterceptor cwi = new ObjectWithInterceptor(bean, interceptor);
        return cwi;
    }

    public Object cacheGet(String key) {
        logger.fine("getting item from cache with cachekey=" + key);
        Object o = cache.get(key);
        logger.fine("got item from cache=" + o);
        return o;
    }

    private void cachePut(String key, Object newInstance) {
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

    private String getIdentifierForManyToOne(Method getter, List<ItemAttribute> atts) {
        String fk = foreignKey(getter);
        for (ItemAttribute att : atts) {
            if (att.getName().equals(fk)) {
                return att.getValue();
            }
        }
        return null;
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

    String getSetterFromGetter(Method getter) {
        return setterName(attributeName(getter));
    }

    private String setterName(String fieldName) {
        return "set" + StringUtils.capitalize(fieldName);
    }

    public String attributeName(Method getter) {
        return StringUtils.uncapitalize(getter.getName().substring(3));
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
    private <T> void setFieldValue(Class tClass, T newInstance, Method getter, String val) {
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

    public Executor getExecutor() {
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
        List<ItemAndAttributes> ia = QueryImpl.getAttributesFromSdb(qr.getItemList(), getExecutor());
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
}
