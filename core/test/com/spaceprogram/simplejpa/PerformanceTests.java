package com.spaceprogram.simplejpa;

import org.junit.Test;

import javax.persistence.Query;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.List;

/**
 * User: treeder
 * Date: May 14, 2008
 * Time: 3:47:32 PM
 */
public class PerformanceTests extends BaseTestClass{

    @Test
    public void testLazyListRetrievalPerformance() throws InterruptedException, ExecutionException {
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();

        Query query;
        List<MyTestObject> obs;

        long start = System.currentTimeMillis();
        int numItems = 120;
        Future future = null;
        for (int i = 0; i < numItems; i++) {
            MyTestObject object = new MyTestObject();
            object.setName("Scooby doo");
            object.setAge(i);
            System.out.println("persisting " + i);
            future = em.persistAsync(object);
        }

        // let them save
        System.out.println("Waiting for all persists to complete.");
        future.get();
        long duration = System.currentTimeMillis() - start;
        printAndLog("duration of persists=" + duration);

        start = System.currentTimeMillis();
        System.out.println("querying for all objects...");
        query = em.createQuery("select o from MyTestObject o ");
        obs = query.getResultList();
        for (MyTestObject ob : obs) {
            System.out.println("ob=" + ob);
        }
        duration = System.currentTimeMillis() - start;
        printAndLog("duration of retreival and prints=" + duration);

        start = System.currentTimeMillis();
        System.out.println("querying for all objects...");
        query = em.createQuery("select o from MyTestObject o ");
        obs = query.getResultList();
        for (MyTestObject ob : obs) {
            System.out.println("ob=" + ob);
        }
        duration = System.currentTimeMillis() - start;
        printAndLog("duration of retreival and prints after first load=" + duration);

        em.close();
    }

}
