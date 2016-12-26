package asl;

import java.io.UnsupportedEncodingException;
import java.security.*;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * This class represents the different hash functions that can be used
 * to hash the key of the request. As of now only CRC32 and the built-in
 * hash code of the java Objects is supported.
 */
public class HashFunction {

	private String type;
	
	public HashFunction(String type) {
		this.type = type;
	}
	
	public long hash(byte[] key) throws NoSuchAlgorithmException {
		
		if (this.type == "CRC32") {
				Checksum checksum = new CRC32();
				// Update the current checksum with the specified array of bytes
				checksum.update(key, 0, key.length);
	
				return checksum.getValue();
		}
		else if (this.type == "JAVA") {
			try {
				String keyString = new String(key, "ASCII");
				return (long) keyString.hashCode();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			return -1;
			
		}
		else
			return -1;
		}
}


