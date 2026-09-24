package br.com.spring.batch.partitioner.storage;

import br.com.spring.batch.partitioner.config.properties.AppProperties.FolderSettings;
import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;

import org.springframework.stereotype.Component;

@Component
public class BlobPaths {

    public static final String FILE_EXTENSION = ".txt";

    private static final String SEPARATOR = "/";
    private static final String PARTITION_SUFFIX = "_part_%04d" + FILE_EXTENSION;

    private final FolderSettings folders;

    public BlobPaths(FolderSettings folders) {
        this.folders = folders;
    }

    public String inboxPrefix() {
        return folders.inbox() + SEPARATOR;
    }

    public String inboxPath(String fileName) {
        return inboxPrefix() + fileName;
    }

    public String partitionPrefix(ReceivedFileDocument original) {
        return join(original.movementType().folder(), original.movementDate(), original.id()) + SEPARATOR;
    }

    public String partitionFileName(ReceivedFileDocument original, int partitionIndex) {
        return baseName(original.fileName()) + String.format(PARTITION_SUFFIX, partitionIndex);
    }

    public String partitionPath(ReceivedFileDocument original, int partitionIndex) {
        return partitionPrefix(original) + partitionFileName(original, partitionIndex);
    }

    public String processedPath(ReceivedFileDocument original) {
        return join(folders.processed(), original.movementDate(), original.id(), original.fileName());
    }

    public static String fileNameOf(String path) {
        return path.substring(path.lastIndexOf(SEPARATOR) + 1);
    }

    private static String join(String... segments) {
        return String.join(SEPARATOR, segments);
    }

    private static String baseName(String fileName) {
        int extensionStart = fileName.lastIndexOf('.');
        return extensionStart > 0 ? fileName.substring(0, extensionStart) : fileName;
    }
}
