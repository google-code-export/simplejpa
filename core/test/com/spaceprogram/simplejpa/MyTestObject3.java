package com.spaceprogram.simplejpa;

import javax.persistence.Entity;

/**
 * User: treeder
 * Date: Feb 18, 2008
 * Time: 5:26:21 PM
 */
@Entity
public class MyTestObject3 extends MySuperClass{
    private String someField3;

    public String getSomeField3() {
        return someField3;
    }

    public void setSomeField3(String someField3) {
        this.someField3 = someField3;
    }
}
