package com.spaceprogram.simplejpa;

import javax.persistence.EntityManager;
import java.util.concurrent.Future;

/**
 * This is an extended EntityManager interface to add extra functionality and performance enhancements specific
 * to databases like SimpleDB.
 *
 * User: treeder
 * Date: Apr 1, 2008
 * Time: 11:42:43 AM
 */
public interface SimpleEntityManager extends EntityManager {

    /**
     * This is the same as persist(Object), but will return immediately and do the actual
     * save to the database in the background.
     *
     * You can use the Future that is returned to make see if any exceptions occurred while doing the save by
     * calling Future.get() and wrapping that in a try/catch. If an Exception occurred, you will get an
     * ExecutionException and you can call getCause() on that to get the original exception.
     * ie:
     * try {
     *    future.get(); // this will block until the task has completed.
     * } catch (ExecutionException ex){
     *    ex.getCause().printStackTrace();
     * }
     *
     * @param o the object to persist.
     * @return the Future representing the persist task.
     */
    Future persistAsync(Object o);

    Future removeAsync(Object o);

    /**
     * This method will rename an attribute in SimpleDB by iterating through EVERY element in the Domain
     * putting the old attribute value to the new attribute name, then deleting the old one.
     *
     * This can take a long time to complete if the Domain is large.
     * 
     * @param domainClass specifies the Domain
     * @param oldAttributeName the attribute name you want values moved from
     * @param newAttributeName the attribute name you want values moved to
     */
    void renameField(Class domainClass, String oldAttributeName, String newAttributeName);

    /**
     * This will basically change the values for the differentiator column (DTYPE) from
     * the oldClassName to the newClass name.
     * 
     * @param oldClassName
     * @param newClass
     */
    void renameSubclass(String oldClassName, Class newClass);
}
