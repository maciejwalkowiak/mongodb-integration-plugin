package biz.neustar.webmetrics.maven.plugins;

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

import java.io.IOException;
import java.util.Map;

/**
 * @goal start-mongod
 * @requiresProject true
 * @phase pre-integration-test
 * @threadSafe false
 */
public class StartMongodMojo extends AbstractMojo {

    /**
     * Absolute path to mongod executable.
     *
     * @parameter
     */
    private String absolutePathToMongod;

    /**
     * Absolute path to mongod database directory.
     * <p>
     *     This argument will be used for mongod's dbpath argument.
     * </p>
     *
     * @parameter default-value="/data/db"
     */
    private String absolutePathToDatabaseDirectory;

    /**
     * Absolute path to the mongoimport executable.
     *
     * @parameter
     */
    private String absolutePathToMongoimport;

    /**
     * A map of collection names to database init files.
     * <p>
     *     The key is the name of the collection.
     *     The value is the absolute path to a data file to import into mongodb.
     * </p>
     *
     * @parameter
     */
    private Map<String, String> dbInitFiles;

    /**
     * The name of the database into which the data files will be imported.
     *
     * @parameter
     */
    private String databaseName;

    /**
     * The number of seconds to wait between starting mongod and importing the data files.
     * <p>
     *     Generally mongod starts up very quickly therefor 2 seconds should be more than enough.
     * </p>
     *
     * @parameter default-value=2
     */
    private int secondsToWaitForMongodStartup;

    //------------------------------------------------------------------------------------------
    //Implementation of abstract methods
    //------------------------------------------------------------------------------------------

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {

        PluginLog.setLog(getLog());

        MongoManager.getInstance().init(absolutePathToMongod, absolutePathToDatabaseDirectory);

        MongoManager.getInstance().startMongoDb();

        if( dbInitFiles != null && ! dbInitFiles.isEmpty() ){
            wait(secondsToWaitForMongodStartup);
            MongoManager.getInstance().importFile(absolutePathToMongoimport, databaseName, dbInitFiles);
        }
    }

    //------------------------------------------------------------------------------------------
    //Private methods
    //------------------------------------------------------------------------------------------

    private void wait(int seconds) throws MojoExecutionException {

        try{
            getLog().info("Waiting " + secondsToWaitForMongodStartup + " seconds for mongod to start up");
            Thread.sleep(seconds * 1000);
        }
        catch(InterruptedException e){
            throw new MojoExecutionException("Interrupted while waiting for mongod to start up", e);
        }
    }


}
