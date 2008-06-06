package com.spaceprogram.simplejpa;

import org.apache.commons.lang.StringUtils;

import javax.persistence.Column;
import javax.persistence.ManyToOne;
import java.lang.reflect.Method;

/**
 * Utility class for names of items in the database.
 *
 * User: treeder
 * Date: Jun 4, 2008
 * Time: 6:28:19 PM
 */
public class NamingHelper {
    public static String getColumnName(Method getter) {
        if (getter.getAnnotation(Column.class) != null) {
            Column column = getter.getAnnotation(Column.class);
            if (column.name() != null && column.name().length() > 0) {
                String columnName = column.name();
                return columnName;
            }
        }
        if (getter.getAnnotation(ManyToOne.class) != null) {
            return NamingHelper.foreignKey(getter);
        }
        return null;
    }

    public static String foreignKey(Method getter) {
        return foreignKey(attributeName(getter));
    }

    public static String foreignKey(String attName) {
        return attName + "_id";
    }// todo: move all these naming functions into a NameHelper class

    public static String getGetterNameFromSetter(Method setter) {
        return getterName(attributeName(setter));
    }

    public static String setterName(String fieldName) {
        return "set" + StringUtils.capitalize(fieldName);
    }

    public static String attributeName(Method getterOrSetter) {
        return StringUtils.uncapitalize(getterOrSetter.getName().substring(3));
    }

    public static <T> String getterName(String fieldName) {
        return ("get" + StringUtils.capitalize(fieldName));
    }
}
