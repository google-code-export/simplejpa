package com.spaceprogram.simplejpa;

/**
 * Holds stats about the last operation performed. Useful for testing.
 * 
 * User: treeder
 * Date: Apr 9, 2008
 * Time: 8:34:44 PM
 */
public class OpStats {
    private int s3Puts;
    private long s3PutsDuration;
    private int attsPut;
    private long attsPutDuration;
    private long attsDeletedDuration;
    private int attsDeleted;

    public void s3Put(long duration) {
        s3Puts++;
        s3PutsDuration += duration;
    }

    public void attsPut(int numAtts, long duration) {
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
}
