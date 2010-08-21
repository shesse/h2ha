/**
 * (c) St. Hesse,   2008
 *
 * $Id$
 */

package com.shesse.h2ha;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;

/**
 *
 * @author sth
 */
public class ProcessUtils
{
    // /////////////////////////////////////////////////////////
    // Class Members
    // /////////////////////////////////////////////////////////
    /** */
    private static Logger log = Logger.getLogger(ProcessUtils.class);


    // /////////////////////////////////////////////////////////
    // Constructors
    // /////////////////////////////////////////////////////////
    /**
     */
    public ProcessUtils()
    {
        log.debug("ProcessUtils()");
    }

    // /////////////////////////////////////////////////////////
    // Methods
    // /////////////////////////////////////////////////////////
    /**
     * ruft java auf und übergibt den aktuellen
     * Classpath
     * @return 
     * @throws IOException 
     */
    public static Process startJavaProcess(final String logPrefix, List<String> command)
    throws IOException
    {
        List<String> osCommand = new ArrayList<String>();
        
        String jhome = System.getProperty("java.home");
        
        osCommand.add(jhome+"/bin/java");
        
        String cp = System.getProperty("java.class.path");
        osCommand.add("-cp");
        osCommand.add(cp);
        
        osCommand.addAll(command);
        
        return startProcess(logPrefix, osCommand);
        
    }

    /**
     * @throws IOException 
     * 
     */
    public static Process startProcess(final String logPrefix, List<String> command)
    throws IOException
    {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        
        Process proc = pb.start();
        
        final BufferedReader procOutput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        
        Thread forwarder = new Thread() {
            public void run()
            {
                String line;
                try {
                    while ((line = procOutput.readLine()) != null) {
                        log.info(logPrefix+": "+line);
                    }
                    procOutput.close();
                } catch (Throwable x) {
                }
            }
        };
        forwarder.start();
        
        return proc;
    }
    
    
    /**
     * @throws IOException 
     * @throws InterruptedException 
     * 
     */
    public static ExecResult exec(String command)
    throws IOException, InterruptedException
    {
        return exec(Arrays.asList("/bin/sh", "-c", command));
    }
    
    /**
     * 
     * @param command
     * @return
     * @throws IOException
     * @throws InterruptedException 
     */
    public static ExecResult exec(List<String> command)
    throws IOException, InterruptedException
    {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);

        Process proc = pb.start();


        final BufferedReader procOutput = new BufferedReader(new InputStreamReader(proc.getInputStream()));
        final List<String> outputLines= new ArrayList<String>();
        Thread outputReader = new Thread() {
            public void run()
            {
                String line;
                try {
                    while ((line = procOutput.readLine()) != null) {
                        outputLines.add(line);
                     }
                    procOutput.close();
                } catch (Throwable x) {
                }
            }
        };
        outputReader.start();
        
        final BufferedReader procError = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
        
        Thread forwarder = new Thread() {
            public void run()
            {
                String line;
                try {
                    while ((line = procError.readLine()) != null) {
                        log.info("exec: "+line);
                    }
                    procError.close();
               } catch (Throwable x) {
               }
            }
        };
        forwarder.start();
        
        ExecResult result = new ExecResult();
        result.outputLines = outputLines;
        result.exitCode = proc.waitFor();
        return result;
    }
    
  
    // /////////////////////////////////////////////////////////
    // Inner Classes
    // /////////////////////////////////////////////////////////
    /** */
    public static class ExecResult {
        public List<String> outputLines;
        public int exitCode;
    }

}
