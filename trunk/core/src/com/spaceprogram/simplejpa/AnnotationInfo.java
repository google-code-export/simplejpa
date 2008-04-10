package com.spaceprogram.simplejpa;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;

/**
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 1:23:04 PM
 */
public class AnnotationInfo {

    private Annotation[] classAnnotations;
    private Method idMethod;
    private Map<String, Method> getterMap = new HashMap();
    private String discriminatorValue;
    private Class rootClass;
    private Class mainClass;

    public void setClassAnnotations(Annotation[] classAnnotations) {
        this.classAnnotations = classAnnotations;
    }

    public void setIdMethod(Method idMethod) {
        this.idMethod = idMethod;
    }

    public Annotation[] getClassAnnotations() {
        return classAnnotations;
    }

    public Method getIdMethod() {
        return idMethod;
    }

    public void addGetter(Method method) {
        getterMap.put(method.getName(), method);
    }

    public Collection<Method> getGetters() {
        return getterMap.values();
    }

    public Method getGetter(String field) {
        String getterName = EntityManagerSimpleJPA.getterName(field);
        return getterMap.get(getterName);
    }

    public void setDiscriminatorValue(String discriminatorValue) {
        this.discriminatorValue = discriminatorValue;
    }

    public String getDiscriminatorValue() {
        return discriminatorValue;
    }

    public void setRootClass(Class rootClass) {
        this.rootClass = rootClass;
    }

    public Class getRootClass() {
        return rootClass;
    }

    public void setMainClass(Class mainClass) {
        this.mainClass = mainClass;
    }

    public Class getMainClass() {
        return mainClass;
    }
}
