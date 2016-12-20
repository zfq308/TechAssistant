/**
 * @(#)Text1.java
 *
 *
 * @author 
 * @version 1.00 2011/10/12
 */
import java.security.*;
import java.math.*;

public class MD5Util {

    public static String getMD5FromStringBuffer(StringBuffer sbuff) {
        String foo = null;
        byte[] inBytes = sbuff.toString().getBytes();
        try {
            MessageDigest algorithm = MessageDigest.getInstance("MD5");
            algorithm.reset();
            algorithm.update(inBytes);
            byte messageDigest[] = algorithm.digest();

            BigInteger bigInt = new BigInteger(1, messageDigest);
            foo = bigInt.toString(16);

        } catch (NoSuchAlgorithmException nsae) {
            Logs.write("getMD5FromStringBuffer", nsae);
        }
        return foo;
    }
    /*public static void main(String args[])
    {
    String tmp = "";
    for(String t: args)
    {
    tmp += t;
    }

    System.out.println(getMD5FromStringBuffer(new StringBuffer(tmp)));
    }
     * */
}
