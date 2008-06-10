package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.SDBException;

/**
 * User: treeder
 * Date: Jun 9, 2008
 * Time: 11:46:25 PM
 */
public class ExceptionHelper {
    public static boolean isDomainDoesNotExist(SDBException e) {
        return e.getMessage() != null && e.getMessage().contains("The specified domain does not exist");
    }
}
