package journal.io.api.dao;

import java.nio.charset.Charset;

public class ConfigurationFactory {

	public static final String MAGIC_STRING = "J.IO";
	public static final byte[] MAGIC_STRING_BYTES = MAGIC_STRING.getBytes(Charset.forName("UTF-8"));

	private static FileAccessConfiguration fileAccessConfiguration;

	public static FileAccessConfiguration CONFIGURATION() {
		if (fileAccessConfiguration == null) {
//			fileAccessConfiguration = new RandomFileAccessConfiguration();
			fileAccessConfiguration = new BufferedReaderFileAccessConfiguration();
		}
		return fileAccessConfiguration;
	}
}
