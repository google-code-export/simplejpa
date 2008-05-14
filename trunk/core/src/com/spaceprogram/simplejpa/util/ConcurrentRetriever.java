package com.spaceprogram.simplejpa.util;

import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.spaceprogram.simplejpa.ItemAndAttributes;
import com.spaceprogram.simplejpa.ItemCallable;
import com.xerox.amazonws.sdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * User: treeder
 * Date: May 14, 2008
 * Time: 3:11:34 PM
 */
public class ConcurrentRetriever {
    private static boolean parallel = true;

    public static List<ItemAndAttributes> test(EntityManagerSimpleJPA em, Domain domain) throws SDBException, ExecutionException, InterruptedException {
        QueryResult result = domain.listItems();
        return getAttributesFromSdb(result.getItemList(), em.getExecutor());
    }

    public static List<ItemAndAttributes> getAttributesFromSdb(List<Item> items, Executor executor) throws SDBException, InterruptedException, ExecutionException {
        if (parallel) {
            return getParallel(items, executor);
        } else {
            return getSerially(items);
        }
    }

    private static List<ItemAndAttributes> getParallel(List<Item> items, Executor executor) throws InterruptedException, ExecutionException {
        CompletionService<ItemAndAttributes> ecs = new ExecutorCompletionService<ItemAndAttributes>(executor);
        for (Item item : items) {
            Callable callable = new ItemCallable(item);
            ecs.submit(callable);
        }
        List<ItemAndAttributes> ret = new ArrayList<ItemAndAttributes>();
        int n = items.size();
        for (int i = 0; i < n; ++i) {
            ItemAndAttributes r = ecs.take().get();
            if (r != null) {
                ret.add(r);
            }
        }
        return ret;
    }

    private static List<ItemAndAttributes> getSerially(List<Item> items) throws SDBException {
        List<ItemAndAttributes> ret = new ArrayList<ItemAndAttributes>();
        for (Item item : items) {
//            logger.fine("item=" + item.getIdentifier());
            List<ItemAttribute> atts = item.getAttributes();
            ret.add(new ItemAndAttributes(item, atts));
        }
        return ret;
    }
}
