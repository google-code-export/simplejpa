package com.spaceprogram.simplejpa.query;

import com.spaceprogram.simplejpa.AnnotationInfo;
import com.spaceprogram.simplejpa.AsyncSaveTask;
import com.spaceprogram.simplejpa.EntityManagerSimpleJPA;
import com.spaceprogram.simplejpa.LazyList;
import com.spaceprogram.simplejpa.util.AmazonSimpleDBUtil;
import com.xerox.amazonws.sdb.Domain;
import com.xerox.amazonws.sdb.SDBException;
import org.apache.commons.lang.NotImplementedException;

import javax.persistence.FlushModeType;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TemporalType;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Need to support the following:
 * <p/>
 * <p/>
 * - Navigation operator (.) DONE
 * - Arithmetic operators: +, - unary *, / multiplication and division +, - addition and subtraction
 * - Comparison operators : =, >, >=, <, <=, <> (not equal), [NOT] BETWEEN, [NOT] LIKE, [NOT] IN, IS [NOT] NULL, IS [NOT] EMPTY, [NOT] MEMBER [OF]
 * - Logical operators: NOT AND OR
 * <p/>
 * see: http://docs.solarmetric.com/full/html/ejb3_langref.html#ejb3_langref_where
 * <p/>
 * User: treeder
 * Date: Feb 8, 2008
 * Time: 7:33:20 PM
 */
public class QueryImpl implements Query {
    private static Logger logger = Logger.getLogger(QueryImpl.class.getName());
    private EntityManagerSimpleJPA em;
    private JPAQuery q;
    private Map<String, Object> paramMap = new HashMap<String, Object>();

    public static String conditionRegex = "(<>)|(>=)|=|>|(<=)|\\band\\b|\\bor\\b"; //"[(<>)(>=)=>(<=)]+|and|or";
    private Integer maxResults;

    public QueryImpl(EntityManagerSimpleJPA em, JPAQuery q) {
        this.em = em;
        this.q = q;
    }

    public List getResultList() {
        String split[] = q.getFrom().split(" ");
        String obClass = split[0];
        Class tClass = em.ensureClassIsEntity(obClass);
        try {
            // convert to amazon query
            Domain d = em.getDomain(tClass);
            StringBuilder amazonQuery;
            if (q.getFilter() != null) {
                amazonQuery = toAmazonQuery(tClass, q);
                if (amazonQuery == null) {
                    return new ArrayList();
                }
            } else {
                amazonQuery = null;
            }
            AnnotationInfo ai = em.getAnnotationManager().getAnnotationInfo(tClass);
            if (ai.getDiscriminatorValue() != null) {
                if (amazonQuery == null || amazonQuery.length() == 0) {
                    amazonQuery = new StringBuilder();
                } else {
                    amazonQuery.append(" intersection ");
                }
                appendFilter(amazonQuery, "DTYPE", "=", ai.getDiscriminatorValue());
            }
            logger.fine("amazonQuery [" + tClass.getName() + "]= " + amazonQuery);
            String qToSend = amazonQuery != null ? amazonQuery.toString() : null;
            em.incrementQueryCount();
            LazyList ret = new LazyList(em, tClass, qToSend);
            ret.setMaxResults(maxResults);
            return ret;
        } catch (SDBException e) {
            if (e.getMessage() != null && e.getMessage().contains("The specified domain does not exist")) {
                return new ArrayList(); // no need to throw here
            }
            throw new PersistenceException(e);
        } catch (Exception e) {
            throw new PersistenceException(e);
        }
    }

   

    public StringBuilder toAmazonQuery(Class tClass, JPAQuery q) {
        StringBuilder sb = new StringBuilder();
        String where = q.getFilter();
        where = where.trim();
        // now split it into pieces
        List<String> split = splitWhere(where);
        Boolean aok = false;
        for (int i = 0; i < split.size(); i += 3) {
            if (aok && i > 0) {
                String andOr = split.get(i);
                if (andOr.equalsIgnoreCase("OR")) {
                    sb.append(" union ");
                } else {
                    sb.append(" intersection ");
                }
            }
            if (i > 0) {
                i++;
            }
//            System.out.println("sbbefore=" + sb);
            aok = appendCondition(tClass, sb, split.get(i), split.get(i + 1), split.get(i + 2));
//            System.out.println("sbafter=" + sb);
            if (aok == null)
                return null; // todo: only return null if it's an AND query, or's should still continue, but skip the intersection part
        }
        logger.fine("query=" + sb);
        return sb;
    }

    public static List<String> splitWhere(String where) {
        List<String> split = new ArrayList<String>();
        Pattern pattern = Pattern.compile(conditionRegex, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(where);
        int lastIndex = 0;
        while (matcher.find()) {
            String s = where.substring(lastIndex, matcher.start()).trim();
//            System.out.println(s);
//            System.out.println("matcher found: " + matcher.group() + " at " + matcher.start() + " to " + matcher.end());
            split.add(s);
            split.add(matcher.group());
            lastIndex = matcher.end();
        }
        split.add(where.substring(lastIndex).trim());
        return split;
    }

    private Boolean appendCondition(Class tClass, StringBuilder sb, String field, String comparator, String param) {
        AnnotationInfo ai = em.getAnnotationManager().getAnnotationInfo(tClass);

        String fieldSplit[] = field.split("\\.");
        if (fieldSplit.length == 1) {
            field = fieldSplit[0];
            /*try {
                BigDecimal bd = new BigDecimal(field);
            } catch (Exception e) {
//                e.printStackTrace();
            }*/
            if (field.equals(param)) {
                return false;
            }
        } else if (fieldSplit.length == 2) {
            field = fieldSplit[1];
        } else if (fieldSplit.length == 3) {
            // NOTE: ONLY SUPPORTING SECOND LEVEL OF GRAPH RIGHT NOW
            // then we have to reach down the graph here. eg: myOb.ob2.name or myOb.ob2.id
            // if filtering by id, then don't need to query for second object, just add a filter on the id field
            String refObjectField = fieldSplit[1];
            field = fieldSplit[2];
//            System.out.println("field=" + field);
            Method getterForReference = ai.getGetter(refObjectField);
            Class refType = getterForReference.getReturnType();
            AnnotationInfo refAi = em.getAnnotationManager().getAnnotationInfo(refType);
            Method getterForField = refAi.getGetter(field);
//            System.out.println("getter=" + getterForField);
            String columnName = AsyncSaveTask.getColumnName(getterForField);
            String paramValue = getParamValueAsStringForAmazonQuery(param, getterForField);
            logger.fine("paramValue=" + paramValue);
            Method refIdMethod = refAi.getIdMethod();
            if (em.attributeName(refIdMethod).equals(field)) {
                logger.fine("Querying using id field, no second query required.");
                appendFilter(sb, em.foreignKey(refObjectField), comparator, paramValue);
            } else {
                // no id method, so query for other object(s) first, then apply the returned value to the original query.
                // todo: this needs some work (multiple ref objects? multiple params on same ref object?)
                Query sub = em.createQuery("select o from " + refType.getName() + " o where o." + field + " " + comparator + " :paramValue");
                sub.setParameter("paramValue", paramMap.get(paramName(param)));
                List subResults = sub.getResultList();
                List<String> ids = new ArrayList<String>();
                for (Object subResult : subResults) {
                    ids.add(em.getId(subResult));
                }
                if (ids.size() > 0) {
//                    sb.append(" intersection ");
                    appendFilterMultiple(sb, em.foreignKey(refObjectField), "=", ids);
                } else {
                    // no matches so should return nothing right? only if an AND query I guess
                    return null;
                }
            }
            return true;
        } else {
            throw new PersistenceException("Invalid field used in query: " + field);
        }
        logger.fine("field=" + field);
        Method getterForField = ai.getGetter(field);
        if (getterForField == null) {
            throw new PersistenceException("No getter for field: " + field);
        }
        String paramValue = getParamValueAsStringForAmazonQuery(param, getterForField);
        logger.fine("paramValue=" + paramValue);
        logger.fine("comp=[" + comparator + "]");
        String columnName = AsyncSaveTask.getColumnName(getterForField);
        if(columnName == null) columnName = field;
        appendFilter(sb, columnName, comparator, paramValue);
        return true;
    }

    private String getParamValueAsStringForAmazonQuery(String param, Method getterForField) {
        String paramName = paramName(param);
        if (paramName == null) {
            // no colon, so just a value?
            /*  try {
                BigDecimal bd = new BigDecimal(param);
            } catch (Exception e) {
//                e.printStackTrace();
            }*/
            return param;
        }
        Object paramOb = paramMap.get(paramName);
        if (paramOb == null) {
            throw new PersistenceException("parameter is null for: " + paramName);
        }
        Class retType = getterForField.getReturnType();
        if (Integer.class.isAssignableFrom(retType)) {
            Integer x = (Integer) paramOb;
            param = AmazonSimpleDBUtil.encodeRealNumberRange(new BigDecimal(x), AmazonSimpleDBUtil.LONG_DIGITS, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
            logger.fine("encoded int " + x + " to " + param);
        } else if (Long.class.isAssignableFrom(retType)) {
            Long x = (Long) paramOb;
            param = AmazonSimpleDBUtil.encodeRealNumberRange(new BigDecimal(x), AmazonSimpleDBUtil.LONG_DIGITS, EntityManagerSimpleJPA.OFFSET_VALUE).toString();
        } else if (Double.class.isAssignableFrom(retType)) {
            Double x = (Double) paramOb;
            param = AmazonSimpleDBUtil.encodeRealNumberRange(new BigDecimal(x), AmazonSimpleDBUtil.LONG_DIGITS, AmazonSimpleDBUtil.LONG_DIGITS,
                    EntityManagerSimpleJPA.OFFSET_VALUE).toString();
        } else if (BigDecimal.class.isAssignableFrom(retType)) {
            BigDecimal x = (BigDecimal) paramOb;
            param = AmazonSimpleDBUtil.encodeRealNumberRange(x, AmazonSimpleDBUtil.LONG_DIGITS, AmazonSimpleDBUtil.LONG_DIGITS,
                    EntityManagerSimpleJPA.OFFSET_VALUE).toString();
        } else if (Date.class.isAssignableFrom(retType)) {
            Date x = (Date) paramOb;
            param = AmazonSimpleDBUtil.encodeDate(x);
        } else {
            param = paramOb.toString();
        }
        return param;
    }

    private String paramName(String param) {
        int colon = param.indexOf(":");
        if (colon == -1) return null;
        String paramName = param.substring(colon + 1);
        return paramName;
    }

    private void appendFilterMultiple(StringBuilder sb, String field, String comparator, List params) {
        sb.append("[");
        int count = 0;
        for (Object param : params) {
            if (count > 0) {
                sb.append("]").append(" intersection ").append("[");
//                sb.append(" OR ");
            }
            sb.append("'").append(field).append("' ").append(comparator).append(" '").append(param).append("'");
            count++;
        }
        sb.append("]");
    }

    private void appendFilter(StringBuilder sb, String field, String comparator, String param) {
        sb.append("['").append(field).append("' ").append(comparator).append(" '").append(param).append("']");
    }

    public Object getSingleResult() {
        throw new NotImplementedException("TODO");
    }

    public int executeUpdate() {
        throw new NotImplementedException("TODO");
    }

    public Query setMaxResults(int maxResults) {
        this.maxResults = maxResults;
        return this;
    }

    public Query setFirstResult(int i) {
        throw new NotImplementedException("TODO");
    }

    public Query setHint(String s, Object o) {
        throw new NotImplementedException("TODO");
    }

    public Query setParameter(String s, Object o) {
        paramMap.put(s, o);
        return this;
    }

    public Query setParameter(String s, Date date, TemporalType temporalType) {
        throw new NotImplementedException("TODO");
    }

    public Query setParameter(String s, Calendar calendar, TemporalType temporalType) {
        throw new NotImplementedException("TODO");
    }

    public Query setParameter(int i, Object o) {
        throw new NotImplementedException("TODO");
    }

    public Query setParameter(int i, Date date, TemporalType temporalType) {
        throw new NotImplementedException("TODO");
    }

    public Query setParameter(int i, Calendar calendar, TemporalType temporalType) {
        throw new NotImplementedException("TODO");
    }

    public Query setFlushMode(FlushModeType flushModeType) {
        throw new NotImplementedException("TODO");
    }
}
