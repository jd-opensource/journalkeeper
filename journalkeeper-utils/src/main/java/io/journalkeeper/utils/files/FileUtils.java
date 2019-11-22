package io.journalkeeper.utils.files;

import java.io.File;
import java.io.IOException;

/**
 * @author LiYue
 * Date: 2019/11/22
 */
public class FileUtils {
    public static void deleteFolder(File folder) throws IOException {
        if(!folder.exists()) return;
        if(folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteFolder(f);
                    } else {
                        if (!f.delete()) {
                            throw new IOException(
                                    String.format("Delete failed: %s!", f.getAbsolutePath())
                            );
                        }
                    }
                }
            }
        }
        if (!folder.delete()) {
            throw new IOException(
                    String.format("Delete failed: %s!", folder.getAbsolutePath())
            );
        }
    }

}
