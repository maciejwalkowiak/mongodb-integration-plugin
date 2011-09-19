package biz.neustar.webmetrics.maven.plugins;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * @goal stop-mongod
 * @requiresProject true
 * @phase post-integration-test
 * @threadSafe false
 */
public class StopMongodMojo extends AbstractMojo {

    //------------------------------------------------------------------------------------------
    //Implementation of abstract methods
    //------------------------------------------------------------------------------------------

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {

        PluginLog.setLog(getLog());

        MongodManager.getInstance().stopMongoDb();
    }

}
