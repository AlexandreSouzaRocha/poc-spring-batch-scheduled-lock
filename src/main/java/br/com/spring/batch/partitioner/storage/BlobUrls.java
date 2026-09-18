package br.com.spring.batch.partitioner.storage;

import java.time.Duration;

public interface BlobUrls {

    String readableUrl(String path, Duration validity);
}
