package com.jerolba.carpet.labs;

import java.io.File;

import com.jerolba.carpet.io.FileSystemOutputFile;

/**
 * Factory methods for common {@link OutputFileFunction} implementations.
 * <p>
 * Use these to avoid boilerplate when configuring
 * {@code withOutputFileFunction()} on partitioned output builders.
 * <p>
 * Example — write to a local directory with automatic parent creation:
 * <pre>
 *   CarpetOutputBuilder.partitionedByValue(RowData.class)
 *       ...
 *       .withOutputFileFunction(OutputFileFunctions.localFileSystem("/tmp/partition"))
 *       .build();
 * </pre>
 */
public class OutputFileFunctions {

    private OutputFileFunctions() {
    }

    /**
     * Returns an {@link OutputFileFunction} that writes to the local file system.
     * <p>
     * The {@code basePath} is prepended to every partition path produced by the
     * file-name generator. Parent directories are created automatically.
     *
     * @param basePath root directory (e.g. {@code "/tmp/partition"})
     * @return an OutputFileFunction backed by {@link FileSystemOutputFile}
     */
    public static OutputFileFunction localFileSystem(String basePath) {
        String root = basePath.endsWith("/") ? basePath : basePath + "/";
        return path -> {
            File file = new File(root + path);
            file.getParentFile().mkdirs();
            return new FileSystemOutputFile(file);
        };
    }

    /**
     * Returns an {@link OutputFileFunction} that writes to the local file system
     * using a {@link File} as the base directory.
     *
     * @param baseDir root directory
     * @return an OutputFileFunction backed by {@link FileSystemOutputFile}
     */
    public static OutputFileFunction localFileSystem(File baseDir) {
        return localFileSystem(baseDir.getAbsolutePath());
    }

}
