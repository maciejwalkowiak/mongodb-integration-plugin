package biz.neustar.webmetrics.maven.plugins;

import org.apache.maven.plugin.logging.Log;

public class PluginLog {

    private static Log log;

    public static void setLog(Log log){
        PluginLog.log = log;
    }

    public static Log getLog(){
        return log;
    }

}
