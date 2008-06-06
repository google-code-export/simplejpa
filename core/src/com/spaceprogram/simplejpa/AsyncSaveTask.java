package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.Item;
import com.xerox.amazonws.sdb.ItemAttribute;
import com.xerox.amazonws.sdb.SDBException;
import net.sf.cglib.proxy.Callback;
import net.sf.cglib.proxy.Factory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import javax.persistence.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Apr 1, 2008
 * Time: 11:51:16 AM
 */
public class AsyncSaveTask implements Callable {
    private static Logger logger = Logger.getLogger(AsyncSaveTask.class.getName());

    private EntityManagerSimpleJPA em;
    private Object o;
    private String id;
    private boolean newObject;

    public AsyncSaveTask(EntityManagerSimpleJPA entityManager, Object o) {
        this.em = entityManager;
        this.o = o;
        long start = System.currentTimeMillis();
        id = prePersist(o); // could probably move this inside the AsyncSaveTask
        logger.fine("prePersist time=" + (System.currentTimeMillis() - start));

    }

    /**
     * Checks that object is an entity and assigns an ID.
     *
     * @param o
     * @return
     */
    private String prePersist(Object o) {
        em.checkEntity(o);
        // create id if required
        String id = em.getId(o);
        if (id == null) {
            newObject = true;
            id = UUID.randomUUID().toString();
        }
        AnnotationInfo ai = em.getFactory().getAnnotationManager().getAnnotationInfo(o);
        em.setFieldValue(o.getClass(), o, ai.getIdMethod(), id);
        return id;
    }


    public Object call() throws Exception {
        persistOnly(o, id);
        return o;
    }

    protected void persistOnly(Object o, String id) throws SDBException, IllegalAccessException, InvocationTargetException, S3ServiceException, IOException {
        long start = System.currentTimeMillis();
        em.invokeEntityListener(o, newObject ? PrePersist.class : PreUpdate.class);
        AnnotationInfo ai = em.getFactory().getAnnotationManager().getAnnotationInfo(o);
        Domain domain;
        if (ai.getRootClass() != null) {
            domain = em.getDomain(ai.getRootClass());
        } else {
            domain = em.getDomain(o.getClass());
        }
        Item item = domain.getItem(id);
        // now set attributes
        List<ItemAttribute> atts = new ArrayList<ItemAttribute>();
        if (ai.getDiscriminatorValue() != null) {
            atts.add(new ItemAttribute(EntityManagerFactoryImpl.DTYPE, ai.getDiscriminatorValue(), true));
        }

        LazyInterceptor interceptor = null;
        if (o instanceof Factory) {
            Factory factory = (Factory) o;
            for (Callback callback2 : factory.getCallbacks()) {
                if(logger.isLoggable(Level.FINER)) logger.finer("callback=" + callback2);
                if (callback2 instanceof LazyInterceptor) {
                    interceptor = (LazyInterceptor) callback2;
                }
            }
        }
        /*
        don't delete attributes if this is a new object AND don't delete
        atts if it's not dirty AND don't delete if no nulls were set (nulledField on LazyInterceptor)
        */
        Collection<Method> getters = ai.getGetters();
        for (Method getter : getters) {
            Object ob = getter.invoke(o);
            if (ob == null) {
                continue;
            }
            String columnName = NamingHelper.getColumnName(getter);
            if (getter.getAnnotation(ManyToOne.class) != null) {
                // store the id of this object
                String id2 = em.getId(ob);
                atts.add(new ItemAttribute(columnName != null ? columnName : NamingHelper.foreignKey(getter), id2, true));
            } else if (getter.getAnnotation(OneToMany.class) != null) {
                // FORCING BI-DIRECTIONAL RIGHT NOW SO JUST IGNORE
            } else if (getter.getAnnotation(Lob.class) != null) {
                // store in s3
                S3Service s3 = null;
                // todo: need to make sure we only store to S3 if it's changed, too slow.
                logger.fine("putting lob to s3");
                long start3 = System.currentTimeMillis();
                s3 = em.getS3Service();
                S3Bucket bucket = s3.createBucket(em.s3bucketName()); // todo: only do this once per EMFactory
                String s3ObjectId = em.s3ObjectId(id, getter);
                S3Object s3Object = new S3Object(bucket, s3ObjectId);
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos);
                out.writeObject(ob);
                s3Object.setDataInputStream(new ByteArrayInputStream(bos.toByteArray()));
                s3Object = s3.putObject(bucket, s3Object);
                out.close();
                em.getOpStats().s3Put(System.currentTimeMillis() - start3);
                logger.finer("setting lobkeyattribute=" + em.lobKeyAttributeName(columnName, getter) + " - " + s3ObjectId);
                atts.add(new ItemAttribute(columnName != null ? columnName : em.lobKeyAttributeName(columnName, getter), s3ObjectId, true));
            } else if (getter.getAnnotation(Enumerated.class) != null) {
                Enumerated enumerated = getter.getAnnotation(Enumerated.class);
                Class retType = getter.getReturnType();
                EnumType enumType = enumerated.value();
                String toSet = null;
                if (enumType == EnumType.STRING) {
                    toSet = ob.toString();
                } else { // ordinal
                    Object[] enumConstants = retType.getEnumConstants();
                    for (int i = 0; i < enumConstants.length; i++) {
                        Object enumConstant = enumConstants[i];
                        if (enumConstant.toString().equals(ob.toString())) {
                            toSet = Integer.toString(i);
                            break;
                        }
                    }
                }
                if(toSet == null){
                    // should never happen
                    throw new PersistenceException("Enum value is null, couldn't find ordinal match: " + ob);
                }
                atts.add(new ItemAttribute(columnName != null ? columnName : NamingHelper.attributeName(getter), toSet, true));
            } else {
                String toSet = ob != null ? em.padOrConvertIfRequired(ob) : "";
                // todo: throw an exception if this is going to exceed maximum size, suggest using @Lob
                atts.add(new ItemAttribute(columnName != null ? columnName : NamingHelper.attributeName(getter), toSet, true));
            }
        }

        // and now finally send it for storage
        long start2 = System.currentTimeMillis();
        item.putAttributes(atts);
        long duration2 = System.currentTimeMillis() - start2;
        logger.fine("putAttributes time=" + (duration2));
        em.getOpStats().attsPut(atts.size(), duration2);

        // Check for nulled attributes so we can send a delete call
        if (interceptor != null && interceptor.getNulledFields() != null && interceptor.getNulledFields().size() > 0) {
            List<ItemAttribute> attsToDelete = new ArrayList<ItemAttribute>();
            for (String s : interceptor.getNulledFields().keySet()) {
                attsToDelete.add(new ItemAttribute(s, null, true));
            }
            start2 = System.currentTimeMillis();
            item.deleteAttributes(attsToDelete);
            // todo: what about lobs?  need to delete from s3
            duration2 = System.currentTimeMillis() - start2;
            logger.fine("deleteAttributes time=" + (duration2));
            em.getOpStats().attsDeleted(attsToDelete.size(), duration2);
        } else {
            logger.fine("deleteAttributes time= no nulled fields, nothing to delete.");
        }
        em.invokeEntityListener(o, newObject ? PostPersist.class : PostUpdate.class);
        logger.fine("persistOnly time=" + (System.currentTimeMillis() - start));
    }

  
}
