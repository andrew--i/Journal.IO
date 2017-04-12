package journal.io.api.dao;


import java.io.File;
import java.io.FileNotFoundException;

public class FileAccessFactory {
    public FileAccessBase create(File file) {
        try {
            return new RandomAccessFile(file);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("File not found " + file.getAbsolutePath());
        }
    }
}
