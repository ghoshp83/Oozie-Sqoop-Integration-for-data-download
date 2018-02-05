package com.pralay.oozieactivities;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import java.text.SimpleDateFormat;

public class GetUnixTime extends Configured implements Tool {

    //@SuppressWarnings("unused")
    public static void main(String[] args) throws IOException, Exception {
    // TODO Auto-generated method stub

    System.out.println(" GetUnixTime Execution Started: ");
    Configuration conf;

    if ((System.getProperty("oozie.action.conf.xml") != null)) {
        conf = new Configuration(false);
            conf.addResource(new Path("file:///", System
                    .getProperty("oozie.action.conf.xml")));
    }
    else
    {
        conf = new Configuration();
    }


    try
    {
        new GenericOptionsParser(conf, args).getRemainingArgs();
    }
    catch (IOException e)
    {
        e.printStackTrace();
        throw new SchedulerException("Unable to use GenericOptionsParser to parse arguments. ",e);
    }

    /* Read the configuration parameters from the oozie xml file  */
    String date      =  conf.get("date");
    String timeZone  =  conf.get("time.zone");

    System.out.println("Configuration Read: ");
    System.out.println("  Date: "+ date);
    System.out.println("  Time Zone: "+ timeZone);

    String dateTime = date + "000000";
    SimpleDateFormat dfm = new SimpleDateFormat("yyyyMMddHHmmss");
    String endTime = date + "235959";
    long unixtime = 12345678;
    long endUnixtime = 0;
    dfm.setTimeZone(TimeZone.getTimeZone(timeZone));  //Specify the time zone
    try
    {
        unixtime = dfm.parse(dateTime).getTime();
        endUnixtime = dfm.parse(endTime).getTime();
        System.out.println(" Parse Time : "+ dfm.parse(dateTime));
        System.out.println(" End Time : "+ dfm.parse(endTime));
    }
    catch (Exception e)
    {
        e.printStackTrace();
    }

    System.out.println(dateTime + "Corresponding Unix Time Stamp : " + unixtime);
    System.out.println(dateTime + "Corresponding End Time Unix Time Stamp : " + endUnixtime);

    /* Write the job execution status into properties file */
    String oozieProp = System.getProperty("oozie.action.output.properties");
    if (oozieProp != null)
    {
        File propFile = new File(oozieProp);
        Properties props = new Properties();
        props.setProperty("unixTimeStamp", String.valueOf(unixtime));
        props.setProperty("startTime", String.valueOf((long)(unixtime/1000)));
        props.setProperty("endTime", String.valueOf((long)(endUnixtime/1000)));
        OutputStream os = new FileOutputStream(propFile);
        props.store(os, "");
        os.close();
        System.out.println("Unix Time Stamp is written to oozie file");
   }
   else
   {
        System.out.println("oozieProp is null and not able to write job status to output ");
   }
}

@Override
public int run(String[] args) throws Exception
{
   // TODO Auto-generated method stub
   return 0;
}

}