package com.spaceprogram.simplejpa.operations;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.spaceprogram.simplejpa.AnnotationInfo;
import com.spaceprogram.simplejpa.DomainHelper;
import com.spaceprogram.simplejpa.EntityManagerFactoryImpl;
import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.spaceprogram.simplejpa.LazyInterceptor;
import com.spaceprogram.simplejpa.NamingHelper;
import net.sf.cglib.proxy.Factory;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.PersistenceException;
import javax.persistence.PostPersist;
import javax.persistence.PostUpdate;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
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
public class Save implements Callable {
    private static Logger logger = Logger.getLogger(Save.class.getName());

    private EntityManagerSimpleJPA em;
    private Object o;
    private String id;
    private boolean newObject;

    public Save(EntityManagerSimpleJPA entityManager, Object o) {
        this.em = entityManager;
        this.o = o;
        long start = System.currentTimeMillis();
        id = prePersist(o);
        if(logger.isLoggable(Level.FINE)) logger.fine("prePersist time=" + (System.currentTimeMillis() - start));

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
//            System.out.println("new object, setting id");
            AnnotationInfo ai = em.getFactory().getAnnotationManager().getAnnotationInfo(o);
            em.setFieldValue(o.getClass(), o, ai.getIdMethod(), id);
        }
        em.cachePut(id, o);
        return id;
    }


    public Object call() throws Exception {
        try {
            persistOnly(o, id);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("CAUGHT AND RETHROWING");
            throw e;
        }
        return o;
    }

    protected void persistOnly(Object o, String id) throws AmazonClientException, IllegalAccessException, InvocationTargetException, IOException {
        long start = System.currentTimeMillis();
        em.invokeEntityListener(o, newObject ? PrePersist.class : PreUpdate.class);
        AnnotationInfo ai = em.getFactory().getAnnotationManager().getAnnotationInfo(o);
        String domainName;
        if (ai.getRootClass() != null) {
        	domainName = em.getOrCreateDomain(ai.getRootClass());
        } else {
        	domainName = em.getOrCreateDomain(o.getClass());
        }        
//        Item item = DomainHelper.findItemById(this.em.getSimpleDb(), domainName, id);
        // now set attributes
        List<ReplaceableAttribute> attsToPut = new ArrayList<ReplaceableAttribute>();
        List<Attribute> attsToDelete = new ArrayList<Attribute>();
        if (ai.getDiscriminatorValue() != null) {
            attsToPut.add(new ReplaceableAttribute(EntityManagerFactoryImpl.DTYPE, ai.getDiscriminatorValue(), true));
        }

        LazyInterceptor interceptor = null;
        if (o instanceof Factory) {
            Factory factory = (Factory) o;
            /*for (Callback callback2 : factory.getCallbacks()) {
                if(logger.isLoggable(Level.FINER)) logger.finer("callback=" + callback2);
                if (callback2 instanceof LazyInterceptor) {
                    interceptor = (LazyInterceptor) callback2;
                }
            }*/
            interceptor = (LazyInterceptor) factory.getCallback(0);
        }

        Collection<Method> getters = ai.getGetters();
        for (Method getter : getters) {
        	Object ob;
        	try
        	{
        		ob = getter.invoke(o);
        	}
        	catch (Exception e)
        	{
        		throw new PersistenceException("Failed invoking getter: " + getter, e);
        	}
        	
            String columnName = NamingHelper.getColumnName(getter);
            if (ob == null) {
                attsToDelete.add(new Attribute(columnName, null));
                continue;
            }
            if (getter.getAnnotation(ManyToOne.class) != null) {
                // store the id of this object
                String id2 = em.getId(ob);
                attsToPut.add(new ReplaceableAttribute(columnName, id2, true));
            } else if (getter.getAnnotation(OneToMany.class) != null) {
                // FORCING BI-DIRECTIONAL RIGHT NOW SO JUST IGNORE
            } else if (getter.getAnnotation(Lob.class) != null) {
                // store in s3
                AmazonS3 s3 = null;
                // todo: need to make sure we only store to S3 if it's changed, too slow.
                logger.fine("putting lob to s3");
                long start3 = System.currentTimeMillis();
                s3 = em.getS3Service();
                String bucketName = em.getS3BucketName(); 
                String s3ObjectId = em.s3ObjectId(id, getter);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos);
                out.writeObject(ob);
                byte[] contentBytes = bos.toByteArray();
                out.close();
                InputStream input = new ByteArrayInputStream(contentBytes);
                
                s3.putObject(bucketName, s3ObjectId, input, null);
                
                em.statsS3Put(System.currentTimeMillis() - start3);
                logger.finer("setting lobkeyattribute=" + columnName + " - " + s3ObjectId);
                attsToPut.add(new ReplaceableAttribute(columnName, s3ObjectId, true));
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
                if (toSet == null) {
                    // should never happen
                    throw new PersistenceException("Enum value is null, couldn't find ordinal match: " + ob);
                }
                attsToPut.add(new ReplaceableAttribute(columnName, toSet, true));
            } 
            else if(getter.getAnnotation(Id.class) != null)
            {
            	continue;
            }
            else {
                String toSet = ob != null ? em.padOrConvertIfRequired(ob) : "";
                // todo: throw an exception if this is going to exceed maximum size, suggest using @Lob
                attsToPut.add(new ReplaceableAttribute(columnName, toSet, true));
            }
        }

        // Now finally send it for storage (If have attributes to add)
        long start2 = System.currentTimeMillis();
        long duration2;
        if (!attsToPut.isEmpty()) {        
            this.em.getSimpleDb().putAttributes(new PutAttributesRequest()
               .withDomainName(domainName)
               .withItemName(id)
               .withAttributes(attsToPut));
            duration2 = System.currentTimeMillis() - start2;
            if(logger.isLoggable(Level.FINE))logger.fine("putAttributes time=" + (duration2));
            em.statsAttsPut(attsToPut.size(), duration2);
        }

        /*
         Check for nulled attributes so we can send a delete call.
        Don't delete attributes if this is a new object
        AND don't delete atts if it's not dirty
        AND don't delete if no nulls were set (nulledField on LazyInterceptor)
        */
        if (interceptor != null) {
            if (interceptor.getNulledFields() != null && interceptor.getNulledFields().size() > 0) {
                List<Attribute> attsToDelete2 = new ArrayList<Attribute>();
                for (String s : interceptor.getNulledFields().keySet()) {
                    Method getter = ai.getGetter(s);
                    String columnName = NamingHelper.getColumnName(getter);
                    attsToDelete2.add(new Attribute(columnName, null));
                }
                start2 = System.currentTimeMillis();
                this.em.getSimpleDb().deleteAttributes(new DeleteAttributesRequest()
                	.withDomainName(domainName)
                	.withItemName(id)
                	.withAttributes(attsToDelete2));
                
                // todo: what about lobs?  need to delete from s3
                duration2 = System.currentTimeMillis() - start2;
                logger.fine("deleteAttributes time=" + (duration2));
                em.statsAttsDeleted(attsToDelete2.size(), duration2);
            } else {
                logger.fine("deleteAttributes time= no nulled fields, nothing to delete.");
            }
        } else {
            if (!newObject && attsToDelete.size() > 0) {
                // not enhanced, but still have to deal with deleted attributes
                start2 = System.currentTimeMillis();
//                for (ItemAttribute itemAttribute : attsToDelete) {
//                    System.out.println("itemAttr=" + itemAttribute.getName() + ": " + itemAttribute.getValue());
//                }
                this.em.getSimpleDb().deleteAttributes(new DeleteAttributesRequest()
	            	.withDomainName(domainName)
	            	.withItemName(id)
	            	.withAttributes(attsToDelete));
                // todo: what about lobs?  need to delete from s3
                duration2 = System.currentTimeMillis() - start2;
                logger.fine("deleteAttributes time=" + (duration2));
                em.statsAttsDeleted(attsToDelete.size(), duration2);
            }
        }
        if (interceptor != null) {
            // reset the interceptor since we're all synced with the db now
            interceptor.reset();
        }
        em.invokeEntityListener(o, newObject ? PostPersist.class : PostUpdate.class);
        if(logger.isLoggable(Level.FINE)) logger.fine("persistOnly time=" + (System.currentTimeMillis() - start));
    }


}
