package journal.io.api.dao;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class FileAccessFactory {
    public FileAccess create(File file) {
        try {
//            return new RandomAccessFile(file, ConfigurationFactory.CONFIGURATION());
            return new BufferedReaderAccessFile(file, ConfigurationFactory.CONFIGURATION());
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not found " + file.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
