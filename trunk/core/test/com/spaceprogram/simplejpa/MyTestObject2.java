package com.spaceprogram.simplejpa;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToOne;

/**
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 7:55:18 PM
 */
@Entity
public class MyTestObject2 {
    private String id;
    private String name;
    private Integer someInt;
    private MyTestObject myTestObject;

    public MyTestObject2() {
        System.out.println("constructing testobject2");
    }

    public MyTestObject2(String name, Integer someInt) {

        this.name = name;
        this.someInt = someInt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getSomeInt() {
        return someInt;
    }

    public void setSomeInt(Integer someInt) {
        this.someInt = someInt;
    }

    @Id
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String toString() {
        return new ReflectionToStringBuilder(this).toString();
    }

    @ManyToOne
    public MyTestObject getMyTestObject() {
        return myTestObject;
    }

    public void setMyTestObject(MyTestObject myTestObject) {
        this.myTestObject = myTestObject;
    }
}
