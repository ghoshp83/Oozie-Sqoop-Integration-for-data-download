package com.pralay.oozieactivities;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class YarnFunctions {

    static YarnConfiguration conf;
    static YarnClient yarnClient;
    private ApplicationId appID;

    private static final Logger logger = LoggerFactory.getLogger(YarnFunctions.class);

    public YarnFunctions() {
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    }

    public void closeYarnFunctions() {
        yarnClient.stop();
        try {
            yarnClient.close();
        } catch (IOException e) {
            logger.warn("Could not close YARN Client correctly!\n" + e.getMessage());
        }
    }

    public String getStatusFromYarn(String appName) throws YarnException, IOException {

        String result = "UNKNOWN";
        List<ApplicationReport> appReports = getApplications();

        for (final ApplicationReport ar : appReports) {
            if (appName.equals(ar.getName())) {
                YarnApplicationState state = ar.getYarnApplicationState();
                if (state != null)
                    result = state.name();
                System.out.println(appName + " application is found in YARN and it's status: " + state.name());
                appID = ar.getApplicationId();
            }
        }

        if (result.equals(YarnApplicationState.NEW.name()) || result.equals(YarnApplicationState.NEW_SAVING.name())
                || result.equals(YarnApplicationState.SUBMITTED.name())) {
            result = "ACCEPTED";
        }
        return result;
    }

     public String getStatusFromYarnWithAppID() throws YarnException, IOException {
        String appSts = "";
        YarnApplicationState sts = yarnClient.getApplicationReport(appID).getYarnApplicationState();
        appSts = sts.toString();
        if (appSts.equals(YarnApplicationState.NEW.name()) || appSts.equals(YarnApplicationState.NEW_SAVING.name())
                || appSts.equals(YarnApplicationState.SUBMITTED.name())) {
            appSts = "ACCEPTED";
        }
        return appSts;
    }

     public String getFinalStatusFromYarnWithAppID() throws YarnException, IOException {
         String appSts = "";
         FinalApplicationStatus sts = yarnClient.getApplicationReport(appID).getFinalApplicationStatus();
         appSts = sts.name();
         return appSts;
     }

    private List<ApplicationReport> getApplications() {
        List<ApplicationReport> appReports = new ArrayList<>();

        try {
            appReports = yarnClient.getApplications(EnumSet.of(YarnApplicationState.NEW,
                    YarnApplicationState.NEW_SAVING, YarnApplicationState.SUBMITTED, YarnApplicationState.ACCEPTED,
                    YarnApplicationState.RUNNING));
        } catch (YarnException | IOException e) {
            logger.error("Couldn't get applications from yarn!" + e.getMessage());
        }

        return appReports;
    }
}
