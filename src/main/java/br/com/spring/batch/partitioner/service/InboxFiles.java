package br.com.spring.batch.partitioner.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import br.com.spring.batch.partitioner.model.document.ReceivedFileDocument;
import br.com.spring.batch.partitioner.model.layout.MovementFileName;
import br.com.spring.batch.partitioner.storage.BlobFile;

public record InboxFiles(List<BlobFile> files) {

    public static final String UNKNOWN_GROUP = "desconhecido";

    public InboxFiles {
        files = List.copyOf(files);
    }

    public InboxFiles including(List<ReceivedFileDocument> unfinished) {
        Set<String> listed = files.stream().map(FileClaimService::idOf).collect(Collectors.toSet());
        List<BlobFile> missing = unfinished.stream()
                .filter(file -> !listed.contains(file.id()))
                .map(InboxFiles::sourceBlobOf)
                .toList();
        return new InboxFiles(Stream.concat(files.stream(), missing.stream()).toList());
    }

    public Map<String, List<BlobFile>> byMovementGroup() {
        return files.stream().collect(Collectors.groupingBy(InboxFiles::groupOf, LinkedHashMap::new,
                Collectors.toList()));
    }

    public List<String> fileNames() {
        return files.stream().map(BlobFile::fileName).toList();
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }

    public int size() {
        return files.size();
    }

    private static BlobFile sourceBlobOf(ReceivedFileDocument file) {
        return new BlobFile(file.blob().sourcePath(), file.sizeBytes(), file.blob().etag());
    }

    static String groupOf(BlobFile file) {
        return MovementFileName.parse(file.fileName())
                .map(name -> name.type().name().toLowerCase(Locale.ROOT))
                .orElse(UNKNOWN_GROUP);
    }
}
