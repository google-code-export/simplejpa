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
    private Map<String, String> lobKeys;

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
                if (lobKeys != null) {
                    String lobKey = lobKeys.get(em.attributeName(method));
                    logger.finer("ManyToOne key=" + lobKey);
                    if (lobKey == null) {
                        return null;
                    }
                    Class retType = method.getReturnType();
                    logger.fine("loading ManyToOne object for type=" + retType + " with id=" + lobKey);
                    Object toSet = em.find(retType, lobKey);
                    logger.fine("got object for ManyToOne=" + toSet);
                    String setterName = em.getSetterFromGetter(method);
                    Method setter = obj.getClass().getMethod(setterName, retType);
                    setter.invoke(obj, toSet);
                }
            } else if (method.getAnnotation(Lob.class) != null) {
                System.out.println("intercepting lob");
//                Object ob = proxy.invoke(obj, null);
                if (lobKeys != null) {
                    String lobKey = lobKeys.get(em.attributeName(method));
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

    public void putLobKey(String attName, String lobKeyVal) {
        if (lobKeys == null) lobKeys = new HashMap<String, String>();
        lobKeys.put(attName, lobKeyVal);
    }
}
