package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.SDBException;
import com.xerox.amazonws.sdb.SimpleDB;
import com.xerox.amazonws.sdb.QueryResult;
import com.xerox.amazonws.sdb.Item;
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
        // todo: should just delete all items in the domain, would probably be faster
        System.out.println("Deleting all objects created during test...");
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();
        SimpleDB db = em.getSimpleDb();
        deleteAll(em, db, MyTestObject.class);
//        db.deleteDomain(d);
//        d = db.getDomain(em.getDomainName(MyTestObject2.class));
        deleteAll(em, db, MyTestObject2.class);
//        db.deleteDomain(d);
//        d = db.getDomain(em.getDomainName(MyTestObject3.class));
//        db.deleteDomain(d);
        deleteAll(em, db, MyTestObject3.class);
        deleteAll(em, db, MyTestObject4.class);
//        d = db.getDomain(em.getDomainName(MyInheritanceObject1.class));
//        db.deleteDomain(d);
        deleteAll(em, db, MyInheritanceObject1.class);
        deleteAll(em, db, MyInheritanceObject2.class);
        deleteAll(em, db, MyInheritanceObject3.class);
        em.close();
    }

    private void deleteAll(EntityManagerSimpleJPA em, SimpleDB db, Class aClass) throws SDBException {
        Domain d = db.getDomain(em.getDomainName(aClass));
        QueryResult items = null;
        try {
            items = d.listItems();
            deleteAll(items.getItemList());
        } catch (SDBException e) {
            if (!ExceptionHelper.isDomainDoesNotExist(e)) {
                e.printStackTrace();
            }
        }
    }

    private void deleteAll(List<Item> itemList) throws SDBException {
        for (Item item : itemList) {
            item.deleteAttributes(null);
        }
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
