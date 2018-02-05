package com.pralay.oozieactivities;

import mjson.Json;
import org.apache.http.HttpEntity;
import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;

import java.io.IOException;
import java.util.Map;

import static mjson.Json.read;

public class RestFunctions {
    private String ip;
    private String port;

    RestFunctions(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    public String start(String appName) {
        String response = "";
        try {
            response = Request.Get("http://" + ip + ":" + port + "/start?app=" + appName).execute().returnContent().asString();
        } catch (IOException e) {
            System.out.println("Error calling start URL= http://" + ip + ":" + port + "/start?app=" + appName);
            e.printStackTrace();
        }
        return response;
    }

    public void stop(String appName) {
        try {
            Request.Get("http://" + ip + ":" + port + "/stop?app=" + appName).execute();
        } catch (IOException e) {
            System.out.println("Error calling stop URL= http://" + ip + ":" + port + "/stop?app=" + appName);
            e.printStackTrace();
        }
    }

    public void delete(String appName) {
        try {
            Request.Get("http://" + ip + ":" + port + "/delete?app=" + appName).execute();
        } catch (IOException e) {
            System.out.println("Error calling delete URL= http://" + ip + ":" + port + "/delete?app=" + appName);
            e.printStackTrace();
        }
    }

    public String update() {
        String response = "";
        try {
            response = Request.Get("http://" + ip + ":" + port + "/update").execute().returnContent().toString();

        } catch (IOException e) {
            System.out.println("Error calling update URL= http://" + ip + ":" + port + "/update");
            e.printStackTrace();
        }

        return response;
    }

    public Map<String, Json> listApps() throws IOException{

        String appsJson = update();     // get a list of apps
        Json apps = read(appsJson).at("apps");

//        to get a list of app names, use keySet from the map
        return apps.asJsonMap();
    }

    public String details(String appName) {
        String response = "";
        try {
            response = Request.Get("http://" + ip + ":" + port + "/details?app=" + appName).execute().returnContent()
                    .toString();
            // System.out.println(" Response read : " + response);

        } catch (IOException e) {
            System.out.println("Error calling details URL= http://" + ip + ":" + port + "/details?app=" + appName);
            e.printStackTrace();
        }
        return response;
    }

    public String determineStatus(String appName) {
        String status = "STOPPED";
        Json details;
        try {
            details = read(details(appName));

        } catch (Exception e) {
            System.out.println("ERROR: Unable to get the details of the SLI app \n" + e.toString());
            return status;
        }
        status = details.at("status").asString();
        return status;
    }

    public void upload(String appName, Integer num_cont, Integer num_cores, Integer mem, String args, String sparkArgs,
            String mainClass) {
        try {

            MultipartEntityBuilder mp = MultipartEntityBuilder.create();

            if (appName != null)
                mp.addPart("app", new StringBody(appName, ContentType.TEXT_PLAIN));
            if (num_cont != null)
                mp.addPart("num_containers", new StringBody(String.valueOf(num_cont), ContentType.TEXT_PLAIN));
            if (num_cores != null)
                mp.addPart("num_cores", new StringBody(String.valueOf(num_cores), ContentType.TEXT_PLAIN));
            if (mem != null)
                mp.addPart("mem", new StringBody(String.valueOf(mem), ContentType.TEXT_PLAIN));
            if (args != null)
                mp.addPart("args", new StringBody(args, ContentType.TEXT_PLAIN));
            if (sparkArgs != null)
                mp.addPart("sparkconf", new StringBody(sparkArgs, ContentType.TEXT_PLAIN));
            if (mainClass != null)
                mp.addPart("class", new StringBody(mainClass, ContentType.TEXT_PLAIN));

            HttpEntity reqEntity = mp.build();

            Request.Post("http://" + ip + ":" + port + "/upload").body(reqEntity).execute();

        } catch (IOException e) {
            System.out.println("ERROR: Error calling details URL= http://" + ip + ":" + port + "/upload");
            if (appName != null)
                System.out.println("     app=" + appName);
            if (num_cont != null)
                System.out.println("     num_containers=" + String.valueOf(num_cont));
            if (num_cores != null)
                System.out.println("     num_cores=" + String.valueOf(num_cores));
            if (mainClass != null)
                System.out.println("     mem=" + String.valueOf(mem));
            if (args != null)
                System.out.println("     args=" + args);
            if (sparkArgs != null)
                System.out.println("     args=" + sparkArgs);
            if (mem != null)
                System.out.println("     class=" + mainClass);
            e.printStackTrace();
        }
    }
}
