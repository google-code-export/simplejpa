package com.spaceprogram.simplejpa;

import org.junit.Before;
import org.junit.Test;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceException;

/**
 * User: treeder
 * Date: Aug 1, 2008
 * Time: 2:23:53 PM
 */
public class SessionlessTests extends BaseTestClass {

    @Before
    public void makeSessionless() {
        BaseTestClass.factory.setSessionless(true);
    }

    @Test(expected = PersistenceException.class)
    public void failWithSession() {
        BaseTestClass.factory.setSessionless(false);
        getReference();
        BaseTestClass.factory.setSessionless(true);
    }

    @Test
    public void noFailSessionless() {
        getReference();
    }

    private void getReference() {
        EntityManager em = factory.createEntityManager(); // sessionfull EntityManager
        MyTestObject2 ob2 = new MyTestObject2("my ob 2", 123);
        ob2.setMyTestObject(new MyTestObject("referenced object"));
        em.persist(ob2.getMyTestObject());
        em.persist(ob2);
        em.close();
        String id = ob2.getId();

        factory.clearSecondLevelCache();

        em = factory.createEntityManager();
        ob2 = em.find(MyTestObject2.class, id);
        em.close();
        System.out.println("closed EM, getting ref");
        MyTestObject ob3 = ob2.getMyTestObject();
        System.out.println("ob3=" + ob3);
    }

}
