/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.utils.files;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author LiYue
 * Date: 2019/11/22
 */
public class FileUtils {
    public static void deleteFolder(Path path) throws IOException {
        File folder = path.toFile();
        if(!folder.exists()) return;
        if(folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteFolder(f.toPath());
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

    public static List<Path> listAllFiles(Path path) throws IOException {
        try (Stream<Path> walk = Files.walk(path)) {

            return walk.filter(Files::isRegularFile).collect(Collectors.toList());

        }
    }

    public static void dump(Path srcPath, Path destPath) throws IOException {
        List<Path> srcFiles = listAllFiles(srcPath);

        List<Path> destFiles = srcFiles.stream()
                .map(srcPath::relativize)
                .map(destPath::resolve)
                .collect(Collectors.toList());
        Files.createDirectories(destPath);
        for (int i = 0; i < destFiles.size(); i++) {
            Path srcFile = srcFiles.get(i);
            Path destFile = destFiles.get(i);
            Files.createDirectories(destFile.getParent());
            Files.copy(srcFile, destFile);
        }
    }
}
