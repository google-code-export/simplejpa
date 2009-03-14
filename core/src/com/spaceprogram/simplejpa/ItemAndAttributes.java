package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.ItemAttribute;

import java.util.List;

/**
 * Holds a typica item with its Attributes.
 *
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 7:55:06 PM
 */
public class ItemAndAttributes {
    private SdbItem item;
    private List<ItemAttribute> atts;

    public ItemAndAttributes(SdbItem item, List<ItemAttribute> atts) {
        this.item = item;
        this.atts = atts;
    }

    public SdbItem getItem() {
        return item;
    }

    public void setItem(SdbItem item) {
        this.item = item;
    }

    public List<ItemAttribute> getAtts() {
        return atts;
    }

    public void setAtts(List<ItemAttribute> atts) {
        this.atts = atts;
    }
}
