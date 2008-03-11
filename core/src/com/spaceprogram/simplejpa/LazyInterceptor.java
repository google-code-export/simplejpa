package com.spaceprogram.simplejpa;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import javax.persistence.Lob;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.HashMap;

/**
 * Lazy loader for second class objects that need it.
 *
 * User: treeder
 * Date: Feb 16, 2008
 * Time: 2:38:04 PM
 */
public class LazyInterceptor implements MethodInterceptor {
    private EntityManagerSimpleJPA em;
    private Map<String, String> lobKeys;

    public LazyInterceptor(EntityManagerSimpleJPA em) {

        this.em = em;
    }

    public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
        String name = method.getName();
        if (name.startsWith("get")) {
            Object ret = proxy.invokeSuper(obj, args);
            if(ret != null){
               // then we've already loaded this up
                return ret;
            }
            if (method.getAnnotation(Lob.class) != null) {
                System.out.println("intercepting lob");
//                Object ob = proxy.invoke(obj, null);
                if(lobKeys != null){
                    String lobKey = lobKeys.get(em.attributeName(method));
                    System.out.println("lobKey=" + lobKey);
                    if(lobKey == null){
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
