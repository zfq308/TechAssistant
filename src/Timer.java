/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
 
/**
 *
 * @author Raviraj
 */
public class Timer {

    long startTime;
    long endTime;
    long processTimeMillis;

    public Timer() {
    }

    public void start() {
        startTime = System.currentTimeMillis();
    }

    public void stop() {
        endTime = System.currentTimeMillis();
        processTimeMillis = endTime - startTime;
    }

    public long getProcessTime() {
        return (endTime - startTime);
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void printProcessTime(String comment) {
        System.out.println(comment + " " + processInSecs());
    }

    public double processInSecs() {
        double inSeconds = (double) processTimeMillis / 1000;
        return inSeconds;
    }
    /**public static void main(String args[])
    {
    Timer t = new Timer();
    t.start();
    System.out.println(t.getStartTime());
    try{
    Thread.sleep(52445);
    }
    catch(InterruptedException ie)
    {
    }
    t.stop();
    System.out.println(t.getEndTime());
    System.out.println(t.getProcessTime());
    System.out.println(t.processInSecs());

    }*/
}
