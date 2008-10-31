package com.spaceprogram.simplejpa.stats;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Holds stats about the last operation performed. Useful for testing.
 * 
 * User: treeder
 * Date: Apr 9, 2008
 * Time: 8:34:44 PM
 */
public class OpStats implements Statistics {
    private AtomicInteger puts = new AtomicInteger();
    private AtomicInteger putsDuration = new AtomicInteger();
    private AtomicInteger s3Puts = new AtomicInteger();
    private AtomicLong s3PutsDuration = new AtomicLong();
    private AtomicInteger attsPut = new AtomicInteger();
    private AtomicLong attsPutDuration = new AtomicLong();
    private AtomicLong attsDeletedDuration = new AtomicLong();
    private AtomicInteger attsDeleted = new AtomicInteger();
    private AtomicInteger gets = new AtomicInteger();
    private AtomicLong getsDuration = new AtomicLong();
    public AtomicInteger queries = new AtomicInteger();



    public void s3Put(long duration) {
        s3Puts.incrementAndGet();
        s3PutsDuration.addAndGet(duration);
    }

    public void attsPut(int numAtts, long duration) {
        puts.incrementAndGet();
        attsPut.addAndGet(numAtts);
        attsPutDuration.addAndGet(duration);
    }

    public void attsDeleted(int attsDeleted, long duration) {
        this.attsDeleted.addAndGet(attsDeleted);
        attsDeletedDuration.addAndGet(duration);
    }

    public void got(int numItems, long duration2) {
        gets.addAndGet(numItems);
        getsDuration.addAndGet(duration2);
    }

    public AtomicInteger getPuts() {
        return puts;
    }

    public void setPuts(AtomicInteger puts) {
        this.puts = puts;
    }

    public AtomicInteger getPutsDuration() {
        return putsDuration;
    }

    public void setPutsDuration(AtomicInteger putsDuration) {
        this.putsDuration = putsDuration;
    }

    public AtomicInteger getS3Puts() {
        return s3Puts;
    }

    public void setS3Puts(AtomicInteger s3Puts) {
        this.s3Puts = s3Puts;
    }

    public AtomicLong getS3PutsDuration() {
        return s3PutsDuration;
    }

    public void setS3PutsDuration(AtomicLong s3PutsDuration) {
        this.s3PutsDuration = s3PutsDuration;
    }

    public AtomicInteger getAttsPut() {
        return attsPut;
    }

    public void setAttsPut(AtomicInteger attsPut) {
        this.attsPut = attsPut;
    }

    public AtomicLong getAttsPutDuration() {
        return attsPutDuration;
    }

    public void setAttsPutDuration(AtomicLong attsPutDuration) {
        this.attsPutDuration = attsPutDuration;
    }

    public AtomicLong getAttsDeletedDuration() {
        return attsDeletedDuration;
    }

    public void setAttsDeletedDuration(AtomicLong attsDeletedDuration) {
        this.attsDeletedDuration = attsDeletedDuration;
    }

    public AtomicInteger getAttsDeleted() {
        return attsDeleted;
    }

    public void setAttsDeleted(AtomicInteger attsDeleted) {
        this.attsDeleted = attsDeleted;
    }

    public AtomicInteger getGets() {
        return gets;
    }

    public void setGets(AtomicInteger gets) {
        this.gets = gets;
    }

    public AtomicLong getGetsDuration() {
        return getsDuration;
    }

    public void setGetsDuration(AtomicLong getsDuration) {
        this.getsDuration = getsDuration;
    }

    public AtomicInteger getQueries() {
        return queries;
    }

    public void setQueries(AtomicInteger queries) {
        this.queries = queries;
    }

    public void incrementGets() {
        this.gets.incrementAndGet();
    }
}
