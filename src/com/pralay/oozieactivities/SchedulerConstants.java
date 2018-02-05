package com.pralay.oozieactivities;

public class SchedulerConstants {

    // fixed values for source/target that identify the data records in HDFS
    static public final String RAW_ATTRIBUTE_DATA = "raw";
    static public final String CANONICAL_ATTRIBUTE_DATA = "canonical";
    static public final String DAILY_ATTRIBUTE_DATA = "dpa"; // daily profiler source
    static public final String WEEKLY_ATTRIBUTE_DATA = "wpa"; // weekly profiler source
    static public final String MONTHLY_ATTRIBUTE_DATA = "mpa"; // monthly profiler
    static public final String DCDIR = "dcdir"; // distributed cache dir

    // configured job types
    final public static String RAW_ATTRIBUTE_JOB = "raw";
    final public static String DAILY_PROFILE_JOB = "daily";
    final public static String WEEKLY_PROFILE_JOB = "weekly";
    final public static String MONTHLY_PROFILE_JOB = "monthly";
    final public static String EXPORTER_JOB = "exporter";
    final public static String MVP_PIPELINE_JOB = "mvp-pipeline";

    // directory for date files
    final public static String DATE_FILES = "dateDir";
    final public static String DATE_FILES_DAY = "day";
    final public static String DATE_FILES_WEEK = "week";
    final public static String DATE_FILES_MONTH = "month";

    final public static String JOB_RUNNING = "RUNNING";
    final public static String IP = "";
    final public static String PORT_NUM = "4040";
    final public static String SLEEP_TIME = "1000";
}