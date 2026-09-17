package br.com.spring.batch.partitioner.storage;

import java.util.List;

public interface BlobCatalog {

    List<BlobFile> list(String prefix);

    int deleteByPrefix(String prefix);
}
