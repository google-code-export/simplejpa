package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.*;
import org.junit.*;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

/**
 * User: treeder
 * Date: Apr 3, 2008
 * Time: 2:32:39 PM
 */
public class UtilTests {
    private static EntityManagerFactoryImpl factory;

    @BeforeClass
    public static void setupEntityManagerFactory() throws IOException {
        factory = new EntityManagerFactoryImpl("testunit", null);
        /*
        This doesn't work when not packaged in jar or something.
        (EntityManagerFactoryImpl) Persistence.createEntityManagerFactory("persistenceSDB");
        */
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
    public void rename() throws IOException, ExecutionException, InterruptedException, SDBException {
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();

        Domain d = em.getFactory().getOrCreateDomain(MyTestObject.class);
        String id = "abc123";
        Item item = d.getItem(id);
        List<ItemAttribute> atts = new ArrayList<ItemAttribute>();
        atts.add(new ItemAttribute("id", id, true));
        atts.add(new ItemAttribute("nameOld", "Bullwinkle", true));
        item.putAttributes(atts);
        MyTestObject object;
        object = em.find(MyTestObject.class, id);
        Assert.assertNull(object.getName());
        System.out.println("name before renameField = " + object.getName());
        Assert.assertEquals(id, object.getId());

        em.renameField(MyTestObject.class, "nameOld", "name");
        em.close();

        // now find it again and the name should be good
        em = (EntityManagerSimpleJPA) factory.createEntityManager();
        object = em.find(MyTestObject.class, id);
        Assert.assertEquals("Bullwinkle", object.getName());
        System.out.println("name after renameField = " + object.getName());
        Assert.assertEquals(id, object.getId());

        // now delete object
        em.remove(object);

        // and make sure it's gone
        object = em.find(MyTestObject.class, object.getId());
        Assert.assertNull(object);
        em.close();
    }

    @Test
    public void renameSubclass() throws IOException, ExecutionException, InterruptedException, SDBException {
        EntityManagerSimpleJPA em = (EntityManagerSimpleJPA) factory.createEntityManager();

        Domain d = em.getFactory().getOrCreateDomain(MyInheritanceObject1.class);
        String id = "abc123";
        Item item = d.getItem(id);
        List<ItemAttribute> atts = new ArrayList<ItemAttribute>();
        atts.add(new ItemAttribute("id", id, true));
        atts.add(new ItemAttribute(EntityManagerFactoryImpl.DTYPE, "MyInheritanceObjectOld", true));
        atts.add(new ItemAttribute("fieldInSubClass2", "Bullwinkle", true));
        item.putAttributes(atts);
        MyInheritanceObject1 object;
        /*object = em.find(MyInheritanceObject2.class, id);
        Assert.assertNull(object);
        object = em.find(MyInheritanceObject1.class, id);
        Assert.assertNotNull(object);
        Assert.assertEquals(id, object.getId());*/

        em.renameSubclass("MyInheritanceObjectOld", MyInheritanceObject2.class);
        em.close();

        // now find it again and the name should be good
        em = (EntityManagerSimpleJPA) factory.createEntityManager();
        MyInheritanceObject2 object2 = em.find(MyInheritanceObject2.class, id);
        Assert.assertNotNull(object2);
        Assert.assertEquals("Bullwinkle", object2.getFieldInSubClass2());
        Assert.assertEquals(id, object2.getId());

        // now delete object
        em.remove(object2);

        // and make sure it's gone
        object = em.find(MyInheritanceObject1.class, object2.getId());
        Assert.assertNull(object);
        em.close();
    }

}
