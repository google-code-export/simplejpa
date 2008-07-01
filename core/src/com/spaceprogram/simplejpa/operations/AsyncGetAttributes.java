package com.spaceprogram.simplejpa.operations;

import com.xerox.amazonws.sdb.Item;
import com.spaceprogram.simplejpa.ItemAndAttributes;

import java.util.concurrent.Callable;

/**
 * For our concurrent SimpleDB accesses.
 *
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 7:55:30 PM
 */
public class AsyncGetAttributes implements Callable<ItemAndAttributes> {
    private Item item;

    public AsyncGetAttributes(Item item) {
        this.item = item;
    }

    public ItemAndAttributes call() throws Exception {
        return new ItemAndAttributes(item, item.getAttributes());
    }
}

