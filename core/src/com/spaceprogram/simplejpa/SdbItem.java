package com.spaceprogram.simplejpa;

import com.xerox.amazonws.sdb.ItemAttribute;

import java.util.List;

/**
 *
 * This is hopefully temporary until Typica sorts things out.
 *
 * User: treeder
 * Date: Mar 8, 2009
 * Time: 10:34:27 PM
 */
public interface SdbItem {
    String getIdentifier();

    List<ItemAttribute> getAttributes();
}
