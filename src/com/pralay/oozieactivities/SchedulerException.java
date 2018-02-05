package com.pralay.oozieactivities;

public class SchedulerException extends Exception{

    private static final long serialVersionUID = -1870767173771957567L;

    public SchedulerException() {
        super();
    }


    public SchedulerException(String message) {
        super(message);
    }

       public SchedulerException(String message, Throwable cause) {
        super(message, cause);
    }


    public SchedulerException(Throwable cause) {
        super(cause);
    }


}