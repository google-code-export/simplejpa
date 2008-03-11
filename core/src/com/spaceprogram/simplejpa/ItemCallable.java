package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Item;

import java.util.concurrent.Callable;

/**
 * For our concurrent SimpleDB accesses.
 *
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 7:55:30 PM
 */
public class ItemCallable implements Callable {
    private Item item;

    public ItemCallable(Item item) {
        this.item = item;
    }

    public ItemAndAttributes call() throws Exception {
        return new ItemAndAttributes(item, item.getAttributes());
    }
}

