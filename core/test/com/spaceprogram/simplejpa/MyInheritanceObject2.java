package com.spaceprogram.simplejpa;

import javax.persistence.Entity;

/**
 * User: treeder
 * Date: Feb 19, 2008
 * Time: 11:13:05 PM
 */
@Entity
public class MyInheritanceObject2 extends MyInheritanceObject1 {
    private String fieldInSubClass;

    public String getFieldInSubClass() {
        return fieldInSubClass;
    }

    public void setFieldInSubClass(String fieldInSubClass) {
        this.fieldInSubClass = fieldInSubClass;
    }
}
