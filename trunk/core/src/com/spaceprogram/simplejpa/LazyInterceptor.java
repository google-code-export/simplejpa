package com.spaceprogram.simplejpa;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import javax.persistence.Lob;
import javax.persistence.ManyToOne;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Lazy loader for second class objects that need it.
 * <p/>
 * User: treeder
 * Date: Feb 16, 2008
 * Time: 2:38:04 PM
 */
public class LazyInterceptor implements MethodInterceptor {
    private static Logger logger = Logger.getLogger(LazyInterceptor.class.getName());
    private EntityManagerSimpleJPA em;
    private Map<String, String> foreignKeys;

    public LazyInterceptor(EntityManagerSimpleJPA em) {

        this.em = em;
    }

    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        String name = method.getName();
        if (name.startsWith("get")) {
            Object ret = proxy.invokeSuper(obj, args);
            if (ret != null) {
                // then we've already loaded this up
                return ret;
            }
            if (method.getAnnotation(ManyToOne.class) != null) {
                logger.finer("intercepting many to one");
                if (foreignKeys != null) {
                    String foreignKey = foreignKeys.get(em.attributeName(method));
                    logger.finer("ManyToOne key=" + foreignKey);
                    if (foreignKey == null) {
                        return null;
                    }
                    Class retType = method.getReturnType();
                    logger.fine("loading ManyToOne object for type=" + retType + " with id=" + foreignKey);
                    Object toSet = em.find(retType, foreignKey);
                    logger.fine("got object for ManyToOne=" + toSet);
                    String setterName = em.getSetterFromGetter(method);
                    Method setter = obj.getClass().getMethod(setterName, retType);
                    setter.invoke(obj, toSet);
                }
            } else if (method.getAnnotation(Lob.class) != null) {
                System.out.println("intercepting lob");
//                Object ob = proxy.invoke(obj, null);
                if (foreignKeys != null) {
                    String lobKey = foreignKeys.get(em.attributeName(method));
                    System.out.println("lobKey=" + lobKey);
                    if (lobKey == null) {
                        return null;
                    }
                    Class retType = method.getReturnType();
                    Object toSet = em.getObjectFromS3(lobKey);
                    //                    System.out.println("toset=" + toSet);
                    String setterName = em.getSetterFromGetter(method);
                    Method setter = obj.getClass().getMethod(setterName, retType);
                    setter.invoke(obj, toSet);
                }

            }
        }
        return proxy.invokeSuper(obj, args);
    }

    public void putForeignKey(String attributeName, String foreignKeyVal) {
        if (foreignKeys == null) foreignKeys = new HashMap<String, String>();
        foreignKeys.put(attributeName, foreignKeyVal);
    }
}
