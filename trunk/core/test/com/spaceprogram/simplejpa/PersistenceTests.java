package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.SDBException;
import com.xerox.amazonws.sdb.SimpleDB;
import org.junit.*;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 1:03:57 PM
 */
public class PersistenceTests {
    private static EntityManagerFactoryImpl factory;


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

    @After
    public void deleteAll() throws SDBException {
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

    @Test
    public void listAllObjects() throws IOException, SDBException, ExecutionException, InterruptedException {
        Class c = MyTestObject.class;
        listAllObjects(c);
        listAllObjects(MyTestObject2.class);
    }

    private void listAllObjects(Class c) throws SDBException, InterruptedException, ExecutionException {
        System.out.println("listing all objects of type " + c);
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();
        em.listAllObjectsRaw(c);
        em.close();
    }

    @Test
    public void saveObject() throws IOException {
        EntityManager em = factory.createEntityManager();
        MyTestObject object = makeTestObjects(em);
        em.persist(object);

        object = em.find(MyTestObject.class, object.getId());
        Assert.assertEquals("Scooby doo", object.getName());

        // now delete object
        em.remove(object);

        // and make sure it's gone
        object = em.find(MyTestObject.class, object.getId());
        System.out.println("object=" + object);
        Assert.assertNull(object);
        em.close();
    }

    int counter = 0;

    private MyTestObject makeTestObjects(EntityManager em) {

        MyTestObject object = new MyTestObject();
        object.setName("Some Random Object");
        object.setAge(100);
        em.persist(object); // saving here first to get an ID for bi-directional stuff (temp solution)

        object = new MyTestObject();
        object.setName("Scooby doo");
        object.setAge(12);
        object.setIncome(50507d);
        object.setSomeDouble(44444.55555);
        object.setSomeLong(88888L);
        object.setBirthday(new Date());
        object.setSomeBigDecimal(new BigDecimal("888888.999999999"));
        object.setBigString("this is a huge string");
        em.persist(object); // saving here first to get an ID for bi-directional stuff (temp solution)

        MyTestObject2 ob2 = new MyTestObject2("shaggy1", counter++);
        em.persist(ob2);
        object.setMyTestObject2(ob2);
        ob2 = new MyTestObject2("shaggy2", counter++);
        ob2.setMyTestObject(object);
        em.persist(ob2);
        object.addToMyList(ob2);

        MyTestObject3 ob3 = new MyTestObject3();
        ob3.setSomeField3("querythis");
        object.setMyTestObject3(ob3);
        em.persist(ob3);

        em.persist(object);
        return object;
    }

    @Test
    public void queryObject() throws IOException {

        EntityManager em = factory.createEntityManager();
        Query query;
        List<MyTestObject> obs;
        MyTestObject originalObject = null;

        originalObject = makeTestObjects(em);

        // no params
        query = em.createQuery("select o from " + MyTestObject.class.getName() + " o");
        query.setParameter("age", 12);
        obs = query.getResultList();
        Assert.assertEquals(2, obs.size());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }


        query = em.createQuery("select o from " + MyTestObject.class.getName() + " o where o.age = :age");
        query.setParameter("age", 12);
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }
        Assert.assertEquals(originalObject.getId(), obs.get(0).getId());
        Assert.assertEquals(originalObject.getName(), obs.get(0).getName());
        Assert.assertEquals(originalObject.getIncome(), obs.get(0).getIncome());
        Assert.assertEquals(originalObject.getBirthday(), obs.get(0).getBirthday());
        Assert.assertEquals(originalObject.getSomeLong(), obs.get(0).getSomeLong());
        Assert.assertEquals(originalObject.getSomeDouble(), obs.get(0).getSomeDouble());
        Assert.assertEquals(originalObject.getSomeBigDecimal(), obs.get(0).getSomeBigDecimal());
        Assert.assertEquals(originalObject.getBigString(), obs.get(0).getBigString());
        System.out.println("bigstring=" + obs.get(0).getBigString());
        Assert.assertEquals(originalObject.getAge(), obs.get(0).getAge());
        Assert.assertEquals(1, obs.get(0).getMyList().size());
        Assert.assertEquals(originalObject.getMyList().get(0).getName(), obs.get(0).getMyList().get(0).getName());

        // two filters
        query = em.createQuery("select o from MyTestObject o where o.income = :income and o.age = :age");
        query.setParameter("income", 50507.0);
        query.setParameter("age", 12);
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }
        Assert.assertEquals(originalObject.getId(), obs.get(0).getId());
        Assert.assertEquals(originalObject.getIncome(), obs.get(0).getIncome());
        Assert.assertEquals(obs.get(0).getMyList().size(), 1);
        Assert.assertEquals(originalObject.getMyList().get(0).getName(), obs.get(0).getMyList().get(0).getName());

        // no matches
        query = em.createQuery("select o from MyTestObject o where o.income = :income and o.age = :age");
        query.setParameter("income", 123.0);
        query.setParameter("age", 12);
        obs = query.getResultList();
        Assert.assertEquals(0, obs.size());


        em.close();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void queryOrderBy() {
        EntityManager em = factory.createEntityManager();
        Query query;
        List<MyTestObject> obs;
        query = em.createQuery("select o from " + MyTestObject.class.getName() + " o where o.income = :income and o.age = :age order by o.age");
        em.close();
    }

    @Test
    public void deleteObject() {
        EntityManager em = factory.createEntityManager();

        MyTestObject ob = new MyTestObject();
        ob.setName("some name");
        em.persist(ob);
        em.getTransaction().commit();
        em.close();

        em = factory.createEntityManager();
        // make sure it's saved
        MyTestObject found = em.find(MyTestObject.class, ob.getId());
        Assert.assertEquals(ob.getId(), found.getId());

        // now we'll delete it
        em.remove(found);
        found = em.find(MyTestObject.class, ob.getId());
        Assert.assertNull(found);

        em.close();
    }

    @Test
    public void nullingAttributes() {
        EntityManager em = factory.createEntityManager();
        Query query;
        List<MyTestObject> obs;

        MyTestObject originalObject = makeTestObjects(em);
        System.out.println("age before=" + originalObject.getAge());
        originalObject.setAge(null);
        em.merge(originalObject);

        // now query for it
        MyTestObject fresh = em.find(MyTestObject.class, originalObject.getId());

        Assert.assertEquals(null, fresh.getAge());
        // and other stuff is intact
        Assert.assertEquals(originalObject.getName(), fresh.getName());
        em.close();

    }

    @Test
    public void queryDownGraph() {
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();
        Query query;
        List<MyTestObject> obs;

        MyTestObject originalObject = makeTestObjects(em);

        int queryCountBefore = em.getQueryCount();
        // This should not query for the second object, just use id reference
        query = em.createQuery("select o from MyTestObject o where o.myTestObject2.id = :id2");
        query.setParameter("id2", originalObject.getMyTestObject2().getId());
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        Assert.assertEquals(queryCountBefore + 1, em.getQueryCount());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }

        // This should query for the sub object, then apply it to the first query for a total of 2 queries
        queryCountBefore = em.getQueryCount();
        query = em.createQuery("select o from MyTestObject o where o.myTestObject2.name = :id2");
        query.setParameter("id2", originalObject.getMyTestObject2().getName());
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        Assert.assertEquals(queryCountBefore + 2, em.getQueryCount());
        Assert.assertEquals(originalObject.getMyTestObject2().getName(), obs.get(0).getMyTestObject2().getName());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }

        // now test querying on two different objects down graph
        queryCountBefore = em.getQueryCount();
        query = em.createQuery("select o from MyTestObject o where o.myTestObject2.name = :id2 and o.myTestObject3.someField3 = :field3");
        query.setParameter("id2", originalObject.getMyTestObject2().getName());
        query.setParameter("field3", originalObject.getMyTestObject3().getSomeField3());
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        Assert.assertEquals(queryCountBefore + 3, em.getQueryCount());
        Assert.assertEquals(originalObject.getMyTestObject2().getName(), obs.get(0).getMyTestObject2().getName());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }

        em.close();
    }

    @Test
    public void testInheritance() {
        EntityManager em = factory.createEntityManager();

        MyInheritanceObject1 object1 = new MyInheritanceObject1();
        object1.setField("field value 1");
        em.persist(object1);

        MyInheritanceObject2 object2 = new MyInheritanceObject2();
        object2.setField("field value 2");
        object2.setFieldInSubClass("sub class field 2");
        em.persist(object2);

        MyInheritanceObject3 object3 = new MyInheritanceObject3();
        object3.setField("field value 3");
        object3.setFieldInSubClass3("sub class field 3");
        em.persist(object3);

        em.getTransaction().commit();
        em.close();

        em = factory.createEntityManager();
        // make sure it's saved
        {
            MyInheritanceObject1 found = em.find(MyInheritanceObject1.class, object1.getId());
            Assert.assertEquals(object1.getId(), found.getId());
            Assert.assertTrue(object1.getClass().isAssignableFrom(found.getClass()));
            Assert.assertEquals(object1.getField(), found.getField());
            }
        {
            MyInheritanceObject2 found = em.find(MyInheritanceObject2.class, object2.getId());
            Assert.assertEquals(object2.getId(), found.getId());
            Assert.assertTrue(object2.getClass().isAssignableFrom(found.getClass()));
            Assert.assertEquals(object2.getField(), found.getField());
            Assert.assertEquals(object2.getFieldInSubClass(), found.getFieldInSubClass());
        }
        {
            MyInheritanceObject3 found = em.find(MyInheritanceObject3.class, object3.getId());
            Assert.assertEquals(object3.getId(), found.getId());
            Assert.assertTrue(object3.getClass().isAssignableFrom(found.getClass()));
            Assert.assertEquals(object3.getField(), found.getField());
            Assert.assertEquals(object3.getFieldInSubClass3(), found.getFieldInSubClass3());
        }
        em.close();

        em = factory.createEntityManager();
        Query query = em.createQuery("select o from MyInheritanceObject2 o where o.field = :field");
        query.setParameter("field", object2.getField());
        List<MyInheritanceObject2> obs2 = query.getResultList();
        Assert.assertEquals(1, obs2.size());
        for (MyInheritanceObject2 found : obs2) {
            System.out.println("ob2=" + found);
            Assert.assertTrue(object2.getClass().isAssignableFrom(found.getClass()));
            Assert.assertEquals(object2.getField(), found.getField());
            Assert.assertEquals(object2.getFieldInSubClass(), found.getFieldInSubClass());
        }

        em.close();
    }


    @Test
    public void testDifferentSyntax() {
        EntityManager em = factory.createEntityManager();

         Query query;
        List<MyTestObject> obs;

        MyTestObject originalObject = makeTestObjects(em);

        query = em.createQuery("select o from MyTestObject o where o.myTestObject2.id = :id2 and 1=1");
        query.setParameter("id2", originalObject.getMyTestObject2().getId());
        obs = query.getResultList();
        Assert.assertEquals(1, obs.size());
        for (MyTestObject ob : obs) {
            System.out.println(ob);
            if (ob.getMyList() != null) {
                System.out.println("list not null: " + ob.getMyList().getClass());
                List<MyTestObject2> ob2s = ob.getMyList();
                for (MyTestObject2 ob2 : ob2s) {
                    System.out.println("ob2=" + ob2);
                }
            }
        }

        em.close();
    }

}
