package com.spaceprogram.simplejpa;

/**
 * Holds stats about the last operation performed. Useful for testing.
 * 
 * User: treeder
 * Date: Apr 9, 2008
 * Time: 8:34:44 PM
 */
public class OpStats {
    private int puts;
    private int putsDuration;
    private int s3Puts;
    private long s3PutsDuration;
    private int attsPut;
    private long attsPutDuration;
    private long attsDeletedDuration;
    private int attsDeleted;
    private int gets;
    private long getsDuration;


    public int getPuts() {
        return puts;
    }

    public void setPuts(int puts) {
        this.puts = puts;
    }

    public int getPutsDuration() {
        return putsDuration;
    }

    public void setPutsDuration(int putsDuration) {
        this.putsDuration = putsDuration;
    }

    public void s3Put(long duration) {
        s3Puts++;
        s3PutsDuration += duration;
    }

    public void attsPut(int numAtts, long duration) {
        puts++;
        attsPut += numAtts;
        attsPutDuration += duration;
    }

    public void attsDeleted(int attsDeleted, long duration) {
        this.attsDeleted += attsDeleted;
        attsDeletedDuration += duration;
    }

    public long getAttsDeletedDuration() {
        return attsDeletedDuration;
    }

    public void setAttsDeletedDuration(long attsDeletedDuration) {
        this.attsDeletedDuration = attsDeletedDuration;
    }

    public int getAttsPut() {
        return attsPut;
    }

    public void setAttsPut(int attsPut) {
        this.attsPut = attsPut;
    }

    public long getAttsPutDuration() {
        return attsPutDuration;
    }

    public void setAttsPutDuration(long attsPutDuration) {
        this.attsPutDuration = attsPutDuration;
    }

    public int getS3Puts() {
        return s3Puts;
    }

    public void setS3Puts(int s3Puts) {
        this.s3Puts = s3Puts;
    }

    public long getS3PutsDuration() {
        return s3PutsDuration;
    }

    public void setS3PutsDuration(long s3PutsDuration) {
        this.s3PutsDuration = s3PutsDuration;
    }

    public int getAttsDeleted() {
        return attsDeleted;
    }

    public void setAttsDeleted(int attsDeleted) {
        this.attsDeleted = attsDeleted;
    }

    public void got(int numItems, long duration2) {
        gets += numItems;
        getsDuration += duration2;
    }

    public int getGets() {
        return gets;
    }

    public void setGets(int gets) {
        this.gets = gets;
    }

    public long getGetsDuration() {
        return getsDuration;
    }

    public void setGetsDuration(long getsDuration) {
        this.getsDuration = getsDuration;
    }
}
