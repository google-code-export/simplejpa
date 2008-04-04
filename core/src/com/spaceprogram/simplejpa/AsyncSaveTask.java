package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.Item;
import com.xerox.amazonws.sdb.ItemAttribute;
import com.xerox.amazonws.sdb.SDBException;
import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Apr 1, 2008
 * Time: 11:51:16 AM
 */
public class AsyncSaveTask implements Callable {
    private static Logger logger = Logger.getLogger(AsyncSaveTask.class.getName());

    private EntityManagerSimpleJPA entityManager;
    private Object o;
    private String id;

    public AsyncSaveTask(EntityManagerSimpleJPA entityManager, Object o, String id) {
        this.entityManager = entityManager;
        this.o = o;
        this.id = id;
    }

    public Object call() throws Exception {
        persistOnly(o, id);
        return o;
    }

    protected void persistOnly(Object o, String id) throws SDBException, IllegalAccessException, InvocationTargetException {
        AnnotationInfo ai = entityManager.getFactory().getAnnotationManager().getAnnotationInfo(o);
        Domain domain;
        if (ai.getRootClass() != null) {
            domain = entityManager.getDomain(ai.getRootClass());
        } else {
            domain = entityManager.getDomain(o.getClass());
        }
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
            String attName = entityManager.attributeName(getter);
            if (ob == null) {
                attsToDelete.add(new ItemAttribute(attName, null, true));
                // todo: what about lobs?  need to delete from s3
                continue;
            }
            if (getter.getAnnotation(ManyToOne.class) != null) {
                // store the id of this object
                String id2 = entityManager.getId(ob);
                atts.add(new ItemAttribute(entityManager.foreignKey((Method) getter), id2, true));
            } else if (getter.getAnnotation(OneToMany.class) != null) {
                // FORCING BI-DIRECTIONAL RIGHT NOW SO JUST IGNORE
            } else if (getter.getAnnotation(Lob.class) != null) {
                // store in s3
                S3Service s3 = null;
                try {
                    // todo: need to make sure we only store to S3 if it's changed, too slow.
                    logger.fine("putting lob to s3");
                    s3 = entityManager.getS3Service();
                    S3Bucket bucket = s3.createBucket(entityManager.s3bucketName()); // todo: only do this once per EMFactory
                    String s3ObjectId = entityManager.s3ObjectId(id, getter);
                    S3Object s3Object = new S3Object(bucket, s3ObjectId);
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream out = new ObjectOutputStream(bos);
                    out.writeObject(ob);
                    s3Object.setDataInputStream(new ByteArrayInputStream(bos.toByteArray()));
                    s3Object = s3.putObject(bucket, s3Object);
                    out.close();
                    logger.fine("setting lobkeyattribute=" + entityManager.lobKeyAttributeName(getter) + " - " + s3ObjectId);
                    atts.add(new ItemAttribute(entityManager.lobKeyAttributeName(getter), s3ObjectId, true));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                String toSet = ob != null ? entityManager.padOrConvertIfRequired(ob) : "";
                // todo: throw an exception if this is going to exceed maximum size, suggest using @Lob
                atts.add(new ItemAttribute(entityManager.attributeName(getter), toSet, true));
            }
        }
        // and now finally send it for storage
        long start = System.currentTimeMillis();
        item.putAttributes(atts);
        logger.fine("putAttributes time=" + (System.currentTimeMillis() - start));
        if (attsToDelete.size() > 0) {
            start = System.currentTimeMillis();
            item.deleteAttributes(attsToDelete);
            logger.fine("deleteAttributes time=" + (System.currentTimeMillis() - start));     
        }
    }

}
