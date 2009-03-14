package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.ItemAttribute;

import java.util.List;

/**
 * User: treeder
 * Date: Mar 8, 2009
 * Time: 10:36:54 PM
 */
public class SdbItemImpl implements SdbItem{
    private String id;
    private List<ItemAttribute> list;

    public SdbItemImpl(String id, List<ItemAttribute> list) {

        this.id = id;
        this.list = list;
    }

    public String getIdentifier() {
        return id;
    }

    public List<ItemAttribute> getAttributes() {
        return list;
    }
}
