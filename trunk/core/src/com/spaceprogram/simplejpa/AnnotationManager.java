package com.spaceprogram.simplejpa;

import javax.persistence.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.HashMap;

/**
 * User: treeder
 * Date: Mar 22, 2008
 * Time: 11:39:54 PM
 */
public class AnnotationManager {

    // todo: implement EntityListeners for timestamps
    private Map<String, AnnotationInfo> annotationMap = new HashMap<String, AnnotationInfo>();
    private Map<String, AnnotationInfo> discriminatorMap = new HashMap<String, AnnotationInfo>();

    public AnnotationInfo getAnnotationInfo(Object o) {
        Class c = o.getClass();
        AnnotationInfo ai = getAnnotationInfo(c);
        return ai;
    }

    public Map<String, AnnotationInfo> getAnnotationMap() {
        return annotationMap;
    }


    public AnnotationInfo getAnnotationInfo(Class c) {
        c = stripEnhancerClass(c);
        AnnotationInfo ai = getAnnotationMap().get(c.getName());
        if (ai == null) {
            ai = putAnnotationInfo(c);
        }
        return ai;
    }

    /**
     * This strips the cglib class name out of the enhanced classes.
     *
     * @param c
     * @return
     */
    public static Class stripEnhancerClass(Class c) {
        String className = c.getName();
        className = stripEnhancerClass(className);
        c = getClass(className);
        return c;
    }

    public static Class getClass(String obClass) {
        try {
            Class c = Class.forName(obClass);
            return c;
        } catch (ClassNotFoundException e) {
            throw new PersistenceException(e);
        }
    }


    public static String stripEnhancerClass(String className) {
        int enhancedIndex = className.indexOf("$$EnhancerByCGLIB");
        if (enhancedIndex != -1) {
            className = className.substring(0, enhancedIndex);
        }
        return className;
    }

    /**
     * Gets all the annotation info for a particular class and puts it in our annotation info cache.
     *
     * @param c
     * @return
     */
    public AnnotationInfo putAnnotationInfo(Class c) {
        AnnotationInfo ai;
        ai = new AnnotationInfo();
        ai.setClassAnnotations(c.getAnnotations());
        ai.setMainClass(c);
        Class superClass = c;
        Class rootClass = null;
        while ((superClass = superClass.getSuperclass()) != null) {
            MappedSuperclass mappedSuperclass = (MappedSuperclass) superClass.getAnnotation(MappedSuperclass.class);
            Entity entity = (Entity) superClass.getAnnotation(Entity.class);
            Inheritance inheritance = (Inheritance) superClass.getAnnotation(Inheritance.class);
            /*
            This inheritance stuff might be a problem the way we're doing it on demand. We might need to
            get all the annotation info when first loading the factory so we know all the subclasses and what not.
            For now, just won't use a discriminator when querying on the root class, but can't have multiple levels this way.
             */
            if (mappedSuperclass != null || entity != null) {
                Method[] methods = superClass.getDeclaredMethods();
                putMethods(ai, methods);
                if (entity != null) {
                    // need discriminator column
                    if (inheritance == null) {
                        throw new PersistenceException("Must use the @Inheritance annotation on " + superClass.getName() + " when using inherited entities.");
                    } else {
                        rootClass = superClass;
                    }
                }
            }
        }
        /* Inheritance inheritance = (Inheritance) c.getAnnotation(Inheritance.class);
        */
        if (rootClass != null) {
            ai.setRootClass(rootClass);
            DiscriminatorValue dv = (DiscriminatorValue) c.getAnnotation(DiscriminatorValue.class);
            String discriminatorValue;
            if (dv != null) {
                discriminatorValue = dv.value();
                if (discriminatorValue == null) {
                    throw new PersistenceException("You must specify a value= for @DiscriminatorValue on " + c.getName());
                }
            } else {
                discriminatorValue = c.getSimpleName();
            }
            ai.setDiscriminatorValue(discriminatorValue);
            discriminatorMap.put(discriminatorValue, ai);
        } else {
            ai.setRootClass(c);
        }
        Method[] methods = c.getDeclaredMethods();
        putMethods(ai, methods);
        if (ai.getIdMethod() == null) {
            throw new PersistenceException("No ID method specified for: " + c.getName());
        }
        
        EntityListeners listeners = (EntityListeners) c.getAnnotation(EntityListeners.class);
        if (listeners != null) {
        	putListeners(ai, listeners.value());
        }
        
        getAnnotationMap().put(c.getName(), ai);
        return ai;
    }


    private void putMethods(AnnotationInfo ai, Method[] methods) {
        for (Method method : methods) {
//            logger.fine("method=" + method.getName());
            if (!method.getName().startsWith("get")) continue;
            Id id = method.getAnnotation(Id.class);
            if (id != null) ai.setIdMethod(method);
            Transient transientM = method.getAnnotation(Transient.class);
            if (transientM != null) continue; // we don't save this one
            ai.addGetter(method);
        }
    }
    
    @SuppressWarnings("unchecked")
	private void putListeners(AnnotationInfo ai, Class[] classes) {
    	Map<Class, ClassMethodEntry> listeners = new HashMap<Class, ClassMethodEntry>();
    	
    	// TODO: More than one listener per event cannot be handled like this...
    	
    	for (Class clazz : classes) {
    		for (Method method : clazz.getMethods()) {
    			PrePersist prePersist = method.getAnnotation(PrePersist.class);
    			if (prePersist != null) { listeners.put(PrePersist.class, new ClassMethodEntry(clazz, method)); continue; }
    			PreUpdate preUpdate = method.getAnnotation(PreUpdate.class);
    			if (preUpdate != null) { listeners.put(PreUpdate.class, new ClassMethodEntry(clazz, method)); continue; }
    			PreRemove preRemove = method.getAnnotation(PreRemove.class);
    			if (preRemove != null) { listeners.put(PreRemove.class, new ClassMethodEntry(clazz, method)); continue; }

    			PostLoad postLoad = method.getAnnotation(PostLoad.class);
    			if (postLoad != null) { listeners.put(PostLoad.class, new ClassMethodEntry(clazz, method)); continue; }
    			PostPersist postPersist = method.getAnnotation(PostPersist.class);
    			if (postPersist != null) { listeners.put(PostPersist.class, new ClassMethodEntry(clazz, method)); continue; }
    			PostUpdate postUpdate = method.getAnnotation(PostUpdate.class);
    			if (postUpdate != null) { listeners.put(PostUpdate.class, new ClassMethodEntry(clazz, method)); continue; }
    			PostRemove postRemove = method.getAnnotation(PostRemove.class);
    			if (postRemove != null) { listeners.put(PostRemove.class, new ClassMethodEntry(clazz, method)); continue; }
    		}
    	}

    	ai.setEntityListeners(listeners);
    }

    public AnnotationInfo getAnnotationInfoByDiscriminator(String discriminatorValue) {
        return discriminatorMap.get(discriminatorValue);
    }
    
    public class ClassMethodEntry {
    	private Class clazz;
    	private Method method;
    	
    	public ClassMethodEntry(Class clazz, Method method) {
    		this.clazz = clazz;
    		this.method = method;
    	}
    	
    	public void invoke(Object... args) throws IllegalArgumentException, IllegalAccessException, InvocationTargetException, InstantiationException {
    		this.method.invoke(clazz.newInstance(), args);
    	}
    }
}
