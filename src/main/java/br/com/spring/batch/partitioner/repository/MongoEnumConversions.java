package br.com.spring.batch.partitioner.repository;

import java.util.List;

import br.com.spring.batch.partitioner.model.enums.FileRole;
import br.com.spring.batch.partitioner.model.enums.FileStatus;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

public final class MongoEnumConversions {

    private MongoEnumConversions() {
    }

    public static MongoCustomConversions create() {
        return new MongoCustomConversions(List.of(new FileStatusWriter(), new FileStatusReader(),
                new FileRoleWriter(), new FileRoleReader()));
    }

    @WritingConverter
    private static final class FileStatusWriter implements Converter<FileStatus, String> {

        @Override
        public String convert(FileStatus status) {
            return status.value();
        }
    }

    @ReadingConverter
    private static final class FileStatusReader implements Converter<String, FileStatus> {

        @Override
        public FileStatus convert(String value) {
            return FileStatus.fromValue(value);
        }
    }

    @WritingConverter
    private static final class FileRoleWriter implements Converter<FileRole, String> {

        @Override
        public String convert(FileRole role) {
            return role.value();
        }
    }

    @ReadingConverter
    private static final class FileRoleReader implements Converter<String, FileRole> {

        @Override
        public FileRole convert(String value) {
            return FileRole.fromValue(value);
        }
    }
}
