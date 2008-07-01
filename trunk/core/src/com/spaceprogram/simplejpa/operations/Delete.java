package com.spaceprogram.simplejpa.operations;

import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.xerox.amazonws.sdb.Domain;

import javax.persistence.PostRemove;
import javax.persistence.PreRemove;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Jun 29, 2008
 * Time: 6:01:41 PM
 */
public class Delete implements Callable {
    private static Logger logger = Logger.getLogger(Delete.class.getName());

    private EntityManagerSimpleJPA em;
    private Object toDelete;
    private String id;

    public Delete(EntityManagerSimpleJPA em, Object toDelete) {
        this.em = em;
        this.toDelete = toDelete;
        id = em.getId(toDelete);
        em.cacheRemove(toDelete.getClass(), id);
    }

    public Object call() throws Exception {
        Domain domain = em.getDomain(toDelete.getClass());
        if(logger.isLoggable(Level.FINE)) logger.fine("deleting item with id: " + id);
        em.invokeEntityListener(toDelete, PreRemove.class);
        domain.deleteItem(id);
        em.invokeEntityListener(toDelete, PostRemove.class);
        return toDelete;
    }
}
