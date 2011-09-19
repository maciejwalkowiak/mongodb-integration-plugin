package biz.neustar.webmetrics.maven.plugins;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.MojoExecutionException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MongodManager {

    //------------------------------------------------------------------------------------------
    //Inner classes
    //------------------------------------------------------------------------------------------

    private static class MongoDbManagerHolder {
        public static final MongodManager MONGOD_MANAGER = new MongodManager();
    };

    //------------------------------------------------------------------------------------------
    //Member variables
    //------------------------------------------------------------------------------------------

    private ProcessBuilder mongodProcessBuilder;

    private Process mongodProcess;

    private Lock lock = new ReentrantLock();

    //------------------------------------------------------------------------------------------
    //Constructors
    //------------------------------------------------------------------------------------------

    private MongodManager(){}

    //------------------------------------------------------------------------------------------
    //Class methods
    //------------------------------------------------------------------------------------------

    public static MongodManager getInstance(){
        return MongoDbManagerHolder.MONGOD_MANAGER;
    }

    //------------------------------------------------------------------------------------------
    //Public methods
    //------------------------------------------------------------------------------------------

    public void init(String absolutePathToMongod, String absolutePathToDatabaseDirectory) throws MojoExecutionException {

        Preconditions.checkNotEmpty(absolutePathToMongod, "absolutePathToMongod");
        Preconditions.checkNotEmpty(absolutePathToDatabaseDirectory, "absolutePathToDatabaseDirectory");

        lock.lock();
        try{
            mongodProcessBuilder = new ProcessBuilder(absolutePathToMongod, "--dbpath", absolutePathToDatabaseDirectory);
            mongodProcessBuilder.redirectErrorStream(true);
        }
        finally{
            lock.unlock();
        }
    }

    public void startMongoDb() throws MojoExecutionException {

        lock.lock();
        try{
            if( mongodProcess == null ){
                registerShutdownMongodHook();
                PluginLog.getLog().info("Starting mongod");
                mongodProcess = mongodProcessBuilder.start();
                launchMongodOutputDumper();
            }
        }
        catch(IOException e){
            throw new MojoExecutionException("Failed to execute mongod", e);
        }
        finally{
            lock.unlock();
        }
    }

    public void stopMongoDb(){

        lock.lock();
        try{
            if( mongodProcess != null ){
                PluginLog.getLog().info("Stopping mongod");
                mongodProcess.destroy();
                mongodProcess = null;
            }
        }
        finally{
            lock.unlock();
        }
    }

    public void importFile(String absolutePathToMongoimport, String databaseName, Map<String, String> dbInitFiles) throws MojoExecutionException {

        Preconditions.checkNotEmpty(absolutePathToMongoimport, "absolutePathToMongoimport");
        Preconditions.checkNotEmpty(databaseName, "databaseName");
        Preconditions.checkNotEmpty(dbInitFiles, "dbInitFiles");

        lock.lock();
        try{
            if( mongodProcess == null ){
                PluginLog.getLog().info("mongod must be running in order to import a file");
            }
            else{
                PluginLog.getLog().info("Begining import of db init files into database [" + databaseName + "]");

                for( Map.Entry<String,String> entry : dbInitFiles.entrySet() ){

                    String collection = entry.getKey();
                    String dbInitFile = entry.getValue();

                    PluginLog.getLog().info("Importing [" + dbInitFile + "] into collection [" + collection + "]");

                    ProcessBuilder mongoimportProcessBuilder = new ProcessBuilder(absolutePathToMongoimport, "--db", databaseName, "--collection", collection, dbInitFile);
                    mongoimportProcessBuilder.redirectErrorStream(true);

                    Process mongoimportProcess = mongoimportProcessBuilder.start();
                    launchMongoimportOutputDumper(mongoimportProcess);
                    mongoimportProcess.waitFor();

                    int exitValue = mongoimportProcess.exitValue();
                    if( exitValue == 0 ){
                        PluginLog.getLog().info("File [" + dbInitFile + "] loaded OK");
                    }
                    else{
                        PluginLog.getLog().error("Failed to import file [" + dbInitFile + "]!!! mongoimport exit code [" + exitValue + "].");
                        throw new MojoExecutionException("Failed to load db init file");
                    }
                }
            }
        }
        catch(IOException e){
            throw new MojoExecutionException("Failed to execute mongoimport", e);
        }
        catch(InterruptedException e){
            throw new MojoExecutionException("Interrupted while waiting for mongoimport process to complete", e);
        }
        finally{
            lock.unlock();
        }
    }

    //------------------------------------------------------------------------------------------
    //Private methods
    //------------------------------------------------------------------------------------------

    private void registerShutdownMongodHook(){

        Thread shutdownHook = new Thread(new Runnable(){
            @Override
            public void run() {

                if( mongodProcess != null ){
                    PluginLog.getLog().warn("Mongod will be stopped by shutdown hook!");
                    MongodManager.getInstance().stopMongoDb();
                }
            }
        }, "Mongod shutdown hook");

        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void launchMongodOutputDumper(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    PluginLog.getLog().debug("Starting " + Thread.currentThread().getName() + " thread");
                    IOUtils.copy(mongodProcess.getInputStream(), System.out);
                }
                catch(IOException e){
                    PluginLog.getLog().debug("Exception while reading mongod process output", e);
                }
                finally{
                    PluginLog.getLog().debug(Thread.currentThread().getName() + " thread finished");
                }
            }
        }, "Mongod Output Dumper").start();
    }

    private void launchMongoimportOutputDumper(final Process mongoimportProcess){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    PluginLog.getLog().debug("Starting " + Thread.currentThread().getName() + " thread");
                    IOUtils.copy(mongoimportProcess.getInputStream(), System.out);
                }
                catch(IOException e){
                    PluginLog.getLog().debug("Exception while reading mongoimport process output", e);
                }
                finally{
                    PluginLog.getLog().debug(Thread.currentThread().getName() + " thread finished");
                }
            }
        }, "Mongoimport Output Dumper").start();
    }

}
