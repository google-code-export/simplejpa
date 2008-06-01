package com.spaceprogram.simplejpa;

import com.spaceprogram.simplejpa.query.JPAQuery;
import com.spaceprogram.simplejpa.query.JPAQueryParser;
import com.spaceprogram.simplejpa.query.QueryImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * User: treeder
 * Date: Mar 2, 2008
 * Time: 6:00:24 PM
 */
public class QueryTests {
    @Test
    public void testWhere(){
        JPAQuery query = new JPAQuery();
        JPAQueryParser parser;
        List<String> split;

        parser = new JPAQueryParser(query, ("select o from MyTestObject o where o.myTestObject2.id = :id2 and 1=1 OR o.myTestObject2.name = 'larry'"));
        parser.parse();
        split = QueryImpl.tokenizeWhere(query.getFilter());
        Assert.assertEquals(11, split.size());
        Assert.assertEquals("o.myTestObject2.id = :id2 and 1 = 1 OR o.myTestObject2.name = 'larry' ", toString(split));
        
    }

    private String toString(List<String> split) {
        StringBuilder sb = new StringBuilder();
        for (String s : split) {
            System.out.print(s + " ");
            sb.append(s + " ");
        }
        return sb.toString();
    }
}
