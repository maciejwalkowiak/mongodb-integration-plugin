package biz.neustar.webmetrics.maven.plugins;

import org.apache.maven.plugin.MojoExecutionException;

import java.util.Map;

public class Preconditions {

    public static void checkNotEmpty(String argumentToCheck, String parameterName) throws MojoExecutionException {

        if( argumentToCheck == null || argumentToCheck.isEmpty() )
            throw new MojoExecutionException("Configuration parameter <" + parameterName + "> must be specified");
    }

    public static void checkNotEmpty(Map<?,?> argumentToCheck, String parameterName) throws MojoExecutionException {

        if( argumentToCheck == null || argumentToCheck.isEmpty() )
            throw new MojoExecutionException("Configuration parameter <" + parameterName + "> must be specified");
    }

}
