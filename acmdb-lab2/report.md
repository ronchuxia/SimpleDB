# Design Choices

## Eviction Policy

Eviction policy 使用 LRU 算法。具体实现过程如下：

1. 实现 `HeapPage.isDirty()` 和 `HeapPage.markDirty()`，从而可以判断页是否为脏
2. 实现 `HeapFile.writePage()`，`BufferPool.flushPage()` 和 `BufferPool.flushAllPages()`，从而可以将脏页写回到磁盘文件
3. 使用 `LinkedHashMap` 存储页，启用 `accessOrder` 模式（LRU）
4. 实现 `BufferPool.evictPage()`，从 `LinkedHashMap` 中 flush 并删除 LRU 页
5. 修改 `BufferPool.getPage()`，从而在 LRU 缓存满的时候调用 `BufferPool.evictPage()`




