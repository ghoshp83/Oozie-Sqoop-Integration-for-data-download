package com.pralay.oozieactivities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import mjson.Json;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import com.pralay.oozieactivities.RestFunctions;

public class OozieTaskMonitor extends Configured implements Tool {

    // @SuppressWarnings("unused")
    public static void main(String[] args) throws IOException, Exception {
        // TODO Auto-generated method stub

        System.out.println("OOZIE TASK Monitoring Started ");
        Configuration conf;

        if ((System.getProperty("oozie.action.conf.xml") != null)) {
            conf = new Configuration(false);
            conf.addResource(new Path("file:///", System.getProperty("oozie.action.conf.xml")));
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
            throw new SchedulerException("Unable to use GenericOptionsParser to parse arguments. ", e);
        }

        /* Read the configuration parameters from the oozie xml file */
        String appName = conf.get("app.name");
        String sparkArgs = conf.get("spark.args");
        String mainClass = conf.get("main.class");
        String arguments = conf.get("arguments");
        String arkMasterMachine = conf.get("ark.master");
        Integer numCont = (conf.get("num.cnt") != null) ? Integer.parseInt(conf.get("num.cnt")) : null;
        Integer numCores = (conf.get("num.cores") != null) ? Integer.parseInt(conf.get("num.cores")) : null;
        Integer memory = (conf.get("memory") != null) ? Integer.parseInt(conf.get("memory")) : null;
        Integer maxSleepTime = (conf.get("max.sleeptime") != null) ? Integer.parseInt(conf.get("max.sleeptime")) : null;
        String testVar = conf.get("test.var");

        System.out.println("Configuration Read: ");
        System.out.println("  App Name: " + appName);
        System.out.println("  Main Class : " + mainClass);
        System.out.println("  Ark Master Machine: " + arkMasterMachine);
        System.out.println("  No of Containers: " + numCont);
        System.out.println("  No of Cores: " + numCores);
        System.out.println("  Memory: " + memory);
        System.out.println("  Spark Args: " + sparkArgs);
        System.out.println("  Arguments: " + arguments);
        System.out.println("  MaxSleepTime (from config): " + maxSleepTime + " msecs");
        System.out.println("  Test var value: " + testVar);

        if (maxSleepTime == null)
        {
            maxSleepTime = 20000;
        }
        if (maxSleepTime > 120000)
        {
            maxSleepTime = 120000;
        }

        Integer maxSleepTimeSec = maxSleepTime / 1000;
        System.out.println("Maximum wait time for status update: " + maxSleepTimeSec + " Secs");

        System.out.println("Checking the app name " + appName);
        RestFunctions restFunctions = new RestFunctions(arkMasterMachine, "8080");
        Map<String, Json> appsMap = restFunctions.listApps();

//        the app name could be the full name (name with version) or a partial name
        if (!appsMap.containsKey(appName)) {
//            partial name case - check to see if there's multiple match
            List<String> matchedNames = new ArrayList<>();
            for (String name : appsMap.keySet()) {
                if (name.startsWith(appName)) {
                    matchedNames.add(name);
                }
            }

            if (matchedNames.size() == 0) {
                System.out.println("App name " + appName + " does not match any installed apps, exiting");
                writeOozieStatus("FAILED", "FAILED", "App name " + appName + " does not match any installed apps, exiting");
                System.exit(1);
            }
            else if (matchedNames.size() > 1) {
                System.out.println("App name " + appName +
                        " has multiple matches from the list of installed apps: ");
                for (String s : matchedNames) {
                    System.out.println(s);
                }
                writeOozieStatus("FAILED", "FAILED", "App name " + appName +
                        " has multiple matches from the list of installed apps: (" +
                        Arrays.toString(matchedNames.toArray()) + ") exiting");
                System.out.println("exiting");
                System.exit(1);
            }

//            note - reassigning appName variable
            appName = matchedNames.get(0);
        }

        System.out.println("Matched app name: " + appName);

        System.out.println("Initializing ARK Master and YARN Client");
        YarnFunctions yarnFunctions = new YarnFunctions();

        /* Check the Status of the running application */
        String jobSts = yarnFunctions.getStatusFromYarn(appName);
        System.out.println(appName + " Application status read for the first time through YARN: " + jobSts);

        /* Check if the application is currently running */
        int secCnt = 0;
        while (jobSts.equals("ACCEPTED") || jobSts.equals("RUNNING"))
        {
            try
            {
                Thread.sleep(1000);
                secCnt = secCnt + 1;
                jobSts = yarnFunctions.getStatusFromYarnWithAppID();
                System.out.println("Application status through YARN: " + jobSts);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        /*
         * RestFunction will be used to upload the data through ARK Master and
         * then start the service
         */
        restFunctions.upload(appName, numCont, numCores, memory, arguments, sparkArgs, mainClass);
        int tries = 0;

        String response = "";
        System.out.println("Starting "+appName);
        response = restFunctions.start(appName);
        while (!response.trim().equals("Starting") && tries < 3 )
        {
            tries++;
            System.out.println("WARNING: Application did not start first time - re-try ("+tries+"). Waiting 20 sec and retrying");
            Thread.sleep(20000);
            response = restFunctions.start(appName);
        }
        if (!response.trim().equals("Starting"))
        {
            System.out.println("ERROR: Could not start application after "+tries+" tries.");
            writeOozieStatus("FAILED", "FAILED", "ERROR: Could not start application "
               + appName + " after "+tries+" tries." + " exiting with Failure");
            System.out.println("exiting");
            System.exit(1);

        }
        else
            System.out.println("NEW CONFIGURATION IS UPLOADED and APPLICATION IS STARTED");

        jobSts = yarnFunctions.getStatusFromYarn(appName);
        secCnt = 0;
        /* Check till the application is submitted/accepted or timedout */
        while (jobSts.equals("UNKNOWN") && (secCnt <= maxSleepTimeSec))
        {
            try
            {
                Thread.sleep(1000);
                secCnt = secCnt + 1;
                jobSts = yarnFunctions.getStatusFromYarn(appName);
                System.out.println(" Application status read through YARN after " + secCnt + " Sec: " + jobSts);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }

        if (secCnt > maxSleepTimeSec)
        {
            System.out.println("APPLICATION is not accepted and timed out with status : " + jobSts);
        }
        else
        {
            System.out.println("APPLICATION is ACCEPTED ");
            jobSts = yarnFunctions.getStatusFromYarnWithAppID();
            /* Check if the application is in ACCEPTED/RUNNING status */
            while (jobSts.equals("ACCEPTED") || jobSts.equals("RUNNING"))
            {
                try
                {
                    Thread.sleep(1000);
                    secCnt = secCnt + 1;
                    jobSts = yarnFunctions.getStatusFromYarnWithAppID();
                    System.out.println(" Application status read through YARN after " + secCnt + " Sec: " + jobSts);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
        }

        String finalStatus = yarnFunctions.getFinalStatusFromYarnWithAppID();
        System.out.println("FINAL APPLICATION STATE: " + jobSts + " STATUS: "+finalStatus);
        /* Close Yarn Client */
        yarnFunctions.closeYarnFunctions();

        writeOozieStatus(jobSts, finalStatus, null);
    }

    private static void writeOozieStatus(String jobSts, String finalStatus, String errMessage) throws FileNotFoundException, IOException {
        /* Write the job execution status into properties file */
        String oozieProp = System.getProperty("oozie.action.output.properties");
        if (oozieProp != null)
        {
            File propFile = new File(oozieProp);
            Properties props = new Properties();
            props.setProperty("status", jobSts); // FINISHED, STOPPED, KILLED

            if (!jobSts.equals("FINISHED"))
            {
                if (errMessage != null)
                    props.setProperty("errmsg", errMessage);
                else
                    props.setProperty("errmsg", "Job is not finished successfully");
            }
            else
            {
                props.setProperty("status", finalStatus); // SUCCEEDED, UNDEFINED, FAILED, KILLED
            }
            OutputStream os = new FileOutputStream(propFile);
            props.store(os, "");
            os.close();
            System.out.println("Application Status is written to Oozie File");
        }
        else
        {
            System.out.println("OozieProp is null and not able to write job status to output ");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

}
