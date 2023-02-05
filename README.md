# mentia

A key/value database written in Java. Inspired by [go-leveldb](https://github.com/syndtr/goleveldb)

Usage
```java
    // First configure StoreConfig
    var config = new StoreConfig();
    // true -> faster writes but can lose data if server goes down
    // false -> slower writes but safe data         
    config.setAsyncWrite(true);
    // if async, for every watermark amount of data, in memory is persisted
    config.setIndexJournalFlushWatermark(UtilConstants.MB * 10);
    // how big each segment files are
    config.setSegmentIndexFoldMark(UtilConstants.MB * 200);
    // faster reads at the cost of more memory
    config.setCacheEnabled(true);
    // if cache enabled how big is the cache
    config.setCacheSize(UtilConstants.MB * 5);
    
    // Thread pool for disk access
    ExecutorService diskAccessPool = Executors.newFixedThreadPool(10, Thread.ofVirtual().factory());
    var path = "path_to_store";
    // sample is the store name
    // creates store if not present
    // if present restores state        
    var store = Store.open(path, "sample", config, pool);
    
    // persist
    store.put("key".getBytes(), "val".getBytes());
    
    // retrieve 
    System.out.println(new String(store.get("key".getBytes())));
```

### Performance:
Running [StoreTest.java](https://github.com/jeyaram-a/mentia/blob/main/store/src/test/java/org/jhouse/mentia/store/StoreTest.java) locally on my Windows laptop<br/>
```shell
mvn test
```
Without cache enabled:

```shell
INFO: Writing took 11925 millis. Total 2513777890 bytes (~2.5 GB)
INFO: Read took 2 millis
```

Fresh read:
```shell
INFO: Read took 1 millis
```

### TODO
- [x] Background compaction
- [ ] Compression
- [ ] Encryption
- [ ] Server setup
