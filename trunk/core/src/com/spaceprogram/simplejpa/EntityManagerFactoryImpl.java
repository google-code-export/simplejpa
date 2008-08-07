package com.spaceprogram.simplejpa;

import com.spaceprogram.simplejpa.cache.CacheFactory2;
import com.spaceprogram.simplejpa.cache.NoopCacheFactory;
import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.ListDomainsResult;
import com.xerox.amazonws.sdb.SDBException;
import com.xerox.amazonws.sdb.SimpleDB;
import net.sf.jsr107cache.Cache;
import net.sf.jsr107cache.CacheException;
import org.scannotation.AnnotationDB;
import org.scannotation.ClasspathUrlFinder;

import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceException;
import javax.persistence.spi.PersistenceUnitInfo;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * User: treeder
 * Date: Feb 10, 2008
 * Time: 6:20:23 PM
 */
public class EntityManagerFactoryImpl implements EntityManagerFactory {
    private static Logger logger = Logger.getLogger(EntityManagerFactoryImpl.class.getName());
    /**
     * Whether or not the factory has been closed
     */
    private boolean closed = false;
    /**
     * This is a set of all the objects we found that are marked as @Entity
     */
    private Set<String> entities;
    /**
     * quick access to the entities
     */
    private Map<String, String> entityMap = new HashMap<String, String>();
    /**
     * properties file values
     */
    private Map props;
    /**
     * Stores annotation info about our entities for easy retrieval when needed
     */
    private AnnotationManager annotationManager = new AnnotationManager();
    /**
     * for all the concurrent action.
     * todo: It might make sense to have two executors, one fast one for queries, and one slow one used for slow things like puts/deletes
     */
    private ExecutorService executor;
    /**
     * Also the prefix that will be applied to each Domain
     */
    private String persistenceUnitName;
    /**
     * cache all the domains in sdb
     */
    private List<Domain> domainsList;
    /**
     * same as domainsList, but map access
     */
    private Map<String, Domain> domainMap = new HashMap<String, Domain>();
    
    private int numExecutorThreads = 50;
    public static final String DTYPE = "DTYPE";


    /**
     * Whether to display amazon queries or not.
     */
    private boolean printQueries = false;
    private String awsAccessKey;
    private String awsSecretKey;
    private String cacheFactoryClassname;
    private CacheFactory2 cacheFactory;
    private boolean sessionless;

    /**
     * This one is generally called via the PersistenceProvider.
     *
     * @param persistenceUnitInfo only using persistenceUnitName for now
     * @param props
     */
    public EntityManagerFactoryImpl(PersistenceUnitInfo persistenceUnitInfo, Map props) {
        this(persistenceUnitInfo != null ? persistenceUnitInfo.getPersistenceUnitName() : null, props);
        init(null);
    }

    /**
     * Use this if you want to construct this directly.
     *
     * @param persistenceUnitName used to prefix the SimpleDB domains
     * @param props               should have accessKey and secretKey
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props) {
        this(persistenceUnitName, props, null);
    }

    /**
     * Use this one in web applications, see: http://code.google.com/p/simplejpa/wiki/WebApplications
     *
     * @param persistenceUnitName
     * @param props
     * @param libsToScan          a set of
     */
    public EntityManagerFactoryImpl(String persistenceUnitName, Map props, Set<String> libsToScan) {
        if (persistenceUnitName == null) {
            throw new IllegalArgumentException("Must have a persistenceUnitName!");
        }
        this.persistenceUnitName = persistenceUnitName;
        this.props = props;
        if(props == null){
            try {
                loadProps2();
            } catch (IOException e) {
                throw new PersistenceException(e);
            }
        }
        init(libsToScan);
    }

    private void init(Set<String> libsToScan) {
        awsAccessKey = (String) props.get("accessKey");
        awsSecretKey = (String) props.get("secretKey");
        printQueries = Boolean.valueOf((String) props.get("printQueries"));
        cacheFactoryClassname = (String) props.get("cacheFactory");
        sessionless = Boolean.valueOf((String)props.get("sessionless"));
        if(awsAccessKey == null || awsAccessKey.length() == 0) {
            throw new PersistenceException("AWS Access Key not found. It is a required property.");
        }
         if(awsSecretKey == null || awsSecretKey.length() == 0) {
            throw new PersistenceException("AWS Secret Key not found. It is a required property.");
        }

        try {
            System.out.println("Scanning for entity classes...");
            URL[] urls;
            try {
                urls = ClasspathUrlFinder.findClassPaths();
            } catch (Exception e) {
                System.err.println("CAUGHT");
                e.printStackTrace();
                urls = new URL[0];
            }
            if (libsToScan != null) {
                URL[] urls2 = new URL[urls.length + libsToScan.size()];
                System.arraycopy(urls, 0, urls2, 0, urls.length);
//                urls = new URL[libsToScan.size()];
                int count = 0;
                for (String s : libsToScan) {
                    logger.fine("libinset=" + s);
                    urls2[count + urls.length] = new File(s).toURL();
                    count++;
                }
                urls = urls2;
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

            initSecondLevelCache();

            executor = Executors.newFixedThreadPool(numExecutorThreads);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void initSecondLevelCache() {
        if(cacheFactoryClassname != null){
            try {
                Class<CacheFactory2> cacheFactoryClass = (Class<CacheFactory2>) Class.forName(cacheFactoryClassname);
                cacheFactory = cacheFactoryClass.newInstance();
                cacheFactory.init(props);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if(cacheFactory == null){
            cacheFactory = new NoopCacheFactory();
        }
    }


    /**
     * Call this to load the props from a file in the root of our classpath called: sdb.properties
     *
     * @throws IOException
     * @deprecated don't use this.
     */
    public void loadProps() throws IOException {

    }

    private void loadProps2() throws IOException {
        Properties props2 = new Properties();
        String propsFileName = "/simplejpa.properties";
        InputStream stream = this.getClass().getResourceAsStream(propsFileName);
        if(stream == null){
            throw new FileNotFoundException(propsFileName + " not found on classpath. Could not initialize SimpleJPA.");
        }
        props2.load(stream);
        props = props2;
        stream.close();
    }

    /**
     * @return a new EntityManager for you to use.
     */
    public EntityManager createEntityManager() {
        return new EntityManagerSimpleJPA(this, sessionless);
    }

    public EntityManager createEntityManager(Map map) {
        return createEntityManager();
    }

    public void close() {
        closed = true;
        executor.shutdown();
        cacheFactory.shutdown();
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

    public Domain setupDbDomain(String domainName) {
        try {
            Domain domain = getDomain(domainName);
            if (domain == null) {
                System.out.println("creating domain: " + domainName);
                SimpleDB db = getSimpleDb();
                domain = db.createDomain(domainName);
                domainsList.add(domain);
                domainMap.put(domain.getName(), domain);
            }
            return domain;
        } catch (SDBException e) {
            throw new PersistenceException("Could not create SimpleDB domain.", e);
        }
    }

    public synchronized Domain getDomain(String domainName) {
        if (domainsList == null) {
            getAllDomains();
        }
        return domainMap.get(domainName);
    }

    public synchronized Domain getDomain(Class c) {
        return getDomain(getDomainName(c));
    }

    public Domain getOrCreateDomain(String domainName) {
        Domain d = getDomain(domainName);
        if (d == null) {
            d = setupDbDomain(domainName);
        }
        return d;
    }

    public Domain getOrCreateDomain(Class c) {
        return getOrCreateDomain(getDomainName(c));
    }

    private synchronized List<Domain> getAllDomains() {
        if (domainsList != null) {
            return domainsList;
        }
        try {
            System.out.println("getting all domains");
            SimpleDB db = getSimpleDb();
            ListDomainsResult listDomainsResult = db.listDomains();
            domainsList = listDomainsResult.getDomainList();
            putDomainsInMap(domainsList);
            while (listDomainsResult.getNextToken() != null) {
                listDomainsResult = db.listDomains(listDomainsResult.getNextToken());
                domainsList.addAll(listDomainsResult.getDomainList());
                putDomainsInMap(domainsList);
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
        SimpleDB db = new SimpleDB(awsAccessKey, awsSecretKey);
        db.setSignatureVersion(0); // todo: TEMPORARY UNTIL SDB FIXES THE UNICODE PROBLEM FOR REST QUERIES
        return db;
    }

    public AnnotationManager getAnnotationManager() {
        return annotationManager;
    }

    public String getDomainName(Class<? extends Object> aClass) {
        String className = getRootClassName(aClass);
        return getDomainName(className);
    }

    public String getDomainName(String className) {
        return getPersistenceUnitName() + "-" + className;
    }

    private String getRootClassName(Class<? extends Object> aClass) {
        AnnotationInfo ai = getAnnotationManager().getAnnotationInfo(aClass);
        String className = ai.getRootClass().getSimpleName();
        return className;
    }

    public boolean isPrintQueries() {
        return printQueries;
    }

    public void setPrintQueries(boolean printQueries) {
        this.printQueries = printQueries;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public Cache getCache(Class aClass) {
        try {
            return cacheFactory.createCache(AnnotationManager.stripEnhancerClass(aClass).getName());
        } catch (CacheException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This will turn on sessionless mode which means that you do not need to keep EntityManager's open, nor do
     * you need to close them. But you should ALWAYS use the second level cache in this case.
     *
     * @param sessionless
     */
    public void setSessionless(boolean sessionless) {
        this.sessionless = sessionless;
    }

    public boolean isSessionless() {
        return sessionless;
    }

    public void clearSecondLevelCache() {
        cacheFactory.clearAll();
    }
}
