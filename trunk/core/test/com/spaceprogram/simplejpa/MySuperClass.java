package com.spaceprogram.simplejpa;

import javax.persistence.Id;
import javax.persistence.MappedSuperclass;

/**
 * User: treeder
 * Date: Feb 16, 2008
 * Time: 11:29:51 AM
 */
@MappedSuperclass
public class MySuperClass {

    private String id;

    @Id
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

}
