package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Item;
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
    private Item item;
    private List<ItemAttribute> atts;

    public ItemAndAttributes(Item item, List<ItemAttribute> atts) {
        this.item = item;
        this.atts = atts;
    }

    public Item getItem() {
        return item;
    }

    public void setItem(Item item) {
        this.item = item;
    }

    public List<ItemAttribute> getAtts() {
        return atts;
    }

    public void setAtts(List<ItemAttribute> atts) {
        this.atts = atts;
    }
}
