package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.ListDomainsResult;
import com.xerox.amazonws.sdb.SDBException;
import com.xerox.amazonws.sdb.SimpleDB;
import org.scannotation.AnnotationDB;
import org.scannotation.ClasspathUrlFinder;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitInfo;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 6:20:23 PM
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory {
    private static Logger logger = Logger.getLogger(EntityManagerFactoryImpl.class.getName());
    /** Whether or not the factory has been closed */
    private boolean closed = false;
    /** This is a set of all the objects we found that are marked as @Entity */
    private Set<String> entities;
    /** quick access to the entities */
    private Map<String, String> entityMap = new HashMap<String, String>();
    /** properties file values */
    private Map props;
    /** Stores annotation info about our entities for easy retrieval when needed */
    private AnnotationManager annotationManager = new AnnotationManager();
    /** for all the concurrent action */
    private ExecutorService executor;
    /** Also the prefix that will be applied to each Domain */
    private String persistenceUnitName;
    /** cache all the domains in sdb */
    private List<Domain> domainsList;
    /** same as domainsList, but map access */
    private Map<String, Domain> domainMap = new HashMap<String, Domain>();

    /**
     * This one is generally called via the PersistenceProvider.
     *
     * @param persistenceUnitInfo only using persistenceUnitName for now
     * @param props
     */
    public EntityManagerFactoryImpl(PersistenceUnitInfo persistenceUnitInfo, Map props) {
        if (persistenceUnitInfo == null || persistenceUnitInfo.getPersistenceUnitName() == null) {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }

        persistenceUnitName = persistenceUnitInfo.getPersistenceUnitName();
        this.props = props;
        init(null);
    }

    /**
     * Use this if you want to construct this directly.
     * @param persistenceUnitName used to prefix the SimpleDB domains
     * @param props should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props) {
        this.persistenceUnitName = persistenceUnitName;
        this.props = props;
        init(null);
    }

    /**
     * Use this one in web applications, see: http://code.google.com/p/simplejpa/wiki/WebApplications
     *
     * @param persistenceUnitName
     * @param props
     * @param libsToScan a set of 
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props, Set<String> libsToScan) {
        this.persistenceUnitName = persistenceUnitName;
        this.props = props;
        init(libsToScan);
    }


    private void init(Set<String> libsToScan) {
        try {
            System.out.println("Scanning for entity classes...");
            URL[] urls;
            if (libsToScan != null) {
                urls = new URL[libsToScan.size()];
                int count = 0;
                for (String s : libsToScan) {
                    logger.fine("libinset=" + s);
                    urls[count] = new File(s).toURL();
                    count++;
                }
            } else {
                urls = ClasspathUrlFinder.findClassPaths();
            }
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("classpath urls:");
                for (URL url : urls) {
                    logger.fine(url.toString());
                }
            }
            AnnotationDB annotationDB = new AnnotationDB();
            annotationDB.scanArchives(urls);
            entities = annotationDB.getAnnotationIndex().get(Entity.class.getName());
            if (entities != null) {
                for (String entity : entities) {
                    System.out.println("entity=" + entity);
                    entityMap.put(entity, entity);
                    // also add simple name to it
                    String simpleName = entity.substring(entity.lastIndexOf(".") + 1);
                    entityMap.put(simpleName, entity);
                    Class c = getAnnotationManager().getClass(entity);
                    getAnnotationManager().putAnnotationInfo(c);
                }
            }
            System.out.println("Finished scanning for entity classes.");

            executor = Executors.newFixedThreadPool(50);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Call this to load the props from a file in the root of our classpath called: sdb.properties
     * @throws IOException
     */
    public void loadProps() throws IOException {
        Properties props2 = new Properties();
        props2.load(this.getClass().getResourceAsStream("/sdb.properties"));
        props = props2;
    }

    /**
     *
     * @return a new EntityManager for you to use.
     */
    public EntityManager createEntityManager() {
        return new EntityManagerSimpleJPA(this);
    }

    public EntityManager createEntityManager(Map map) {
        return createEntityManager();
    }

    public void close() {
        closed = true;
        executor.shutdown();
        // todo: clean up any caching, etc
    }

    public boolean isOpen() {
        return !closed;
    }

    public Map<String, String> getEntityMap() {
        return entityMap;
    }

    public Map getProps() {
        return props;
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public String getPersistenceUnitName() {
        return persistenceUnitName;
    }

    public void setPersistenceUnitName(String persistenceUnitName) {
        this.persistenceUnitName = persistenceUnitName;
    }

    public void setupDbDomain(String domainName) {
        try {
            Domain d = getDomain(domainName);
            if (d == null) {
                SimpleDB db = getSimpleDb();
                db.createDomain(domainName);
            }
        } catch (SDBException e) {
            throw new PersistenceException("Could not create SimpleDB domain.", e);
        }
    }

    private synchronized Domain getDomain(String domainName) {
        if (domainsList == null) {
            getAllDomains();
        }
        return domainMap.get(domainName);
    }

    private synchronized List<Domain> getAllDomains() {
        if (domainsList != null) {
            return domainsList;
        }
        try {
            SimpleDB db = getSimpleDb();
            ListDomainsResult listDomainsResult = db.listDomains();
            domainsList = listDomainsResult.getDomainList();
            putDomainsInMap(domainsList);
            while (listDomainsResult.getNextToken() != null) {
                listDomainsResult = db.listDomains(listDomainsResult.getNextToken());
                domainsList.addAll(listDomainsResult.getDomainList());
            }
        } catch (SDBException e) {
            throw new PersistenceException(e);
        }
        return domainsList;
    }

    private void putDomainsInMap(List<Domain> domainsList) {
        for (Domain domain : domainsList) {
            domainMap.put(domain.getName(), domain);
        }
    }

    public SimpleDB getSimpleDb() {
        return getSimpleDb(getProps());
    }

    public SimpleDB getSimpleDb(Map props) {
        SimpleDB db = new SimpleDB((String) props.get("accessKey"), (String) props.get("secretKey"));
        db.setSignatureVersion(0); // TEMPORARY UNTIL SDB FIXES THE UNICODE PROBLEM FOR REST QUERIES
        return db;
    }

    public AnnotationManager getAnnotationManager() {
        return annotationManager;
    }
}
