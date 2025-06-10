package io.github.alikelleci.easysourcing.core.support;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.util.Map;

public class CustomRocksDbConfig implements RocksDBConfigSetter {

  @Override
  public void setConfig(String s, Options options, Map<String, Object> map) {
    options.setCompressionType(CompressionType.ZSTD_COMPRESSION);
//    options.setCompactionStyle(CompactionStyle.LEVEL);
  }

  @Override
  public void close(String storeName, Options options) {
    options.close();
  }
}
