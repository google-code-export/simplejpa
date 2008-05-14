package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.SDBException;
import com.xerox.amazonws.sdb.SimpleDB;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

/**
 * User: treeder
 * Date: May 14, 2008
 * Time: 3:48:37 PM
 */
public class BaseTestClass {
    static EntityManagerFactoryImpl factory;
    List<String> afterTestLog = new ArrayList<String>();

    @BeforeClass
    public static void setupEntityManagerFactory() throws IOException {
        factory = new EntityManagerFactoryImpl("testunit", null);
        factory.loadProps();

        /*
        This doesn't work when not packaged in jar or something.
        (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("persistenceSDB");*/
    }

    @AfterClass
    public static void tearDownEntityManagerFactory() {
        factory.close();
    }

    @Before
    public void cleanupLoggingStuff(){
        afterTestLog.clear();
    }

    @After
    public void deleteAll() throws SDBException {
        printLog();

        System.out.println("Deleting all objects created during test...");
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();
        SimpleDB db = em.getSimpleDb();
        Domain d = db.getDomain(em.getDomainName(MyTestObject.class));
        db.deleteDomain(d);
        d = db.getDomain(em.getDomainName(MyTestObject2.class));
        db.deleteDomain(d);
        d = db.getDomain(em.getDomainName(MyInheritanceObject1.class));
        db.deleteDomain(d);
        em.close();
    }

    private void printLog() {
        for (String s : afterTestLog) {
            System.out.println(s);
        }
    }

    protected void printAndLog(String s) {
        System.out.println(s);
        afterTestLog.add(s);
    }
}
