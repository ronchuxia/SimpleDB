# Design Choices

## Eviction Policy

Eviction policy 使用 LRU 算法。具体实现过程如下：
1. 实现 `HeapPage.isDirty()` 和 `HeapPage.markDirty()`，从而可以判断页是否为脏
2. 实现 `HeapFile.writePage()`，`BufferPool.flushPage()` 和 `BufferPool.flushAllPages()`，从而可以将脏页写回到磁盘文件
3. 使用 `LinkedHashMap` 存储页，启用 `accessOrder` 模式（LRU）
4. 实现 `BufferPool.evictPage()`，从 `LinkedHashMap` 中 flush 并删除 LRU 页
5. 修改 `BufferPool.getPage()`，从而在 LRU 缓存满的时候调用 `BufferPool.evictPage()`

## Search
`BTreeLeafPage findLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId pid, Permissions perm, Field f);`
- 递归查找，返回 key 所在的叶子节点
- 对于存在于两个相邻的叶子节点的 duplicate key，返回它所在的第一个叶子节点
- `f == null` 时返回最左侧的 `BTreeLeafPage`
- 调用 `BTreeFile.getPage()` （是对 `BufferPool.getPage()`的封装），用于记录访问过程中的脏页

## Insertion
`ArrayList<Page> insertTuple(TransactionId tid, Tuple t);`

插入时，可能需要递归地修改父节点。如果修改的父节点数量超过了 BufferPool 的容量，会导致脏页在被最终确定之前就被写回到磁盘。因此，`insertTuple()` 维护一个 local cache，用于缓存插入过程中所有的脏页。在调用 `BTreeFile.readPage()` 时，会自动根据读取页的访问权限（如果是 `Permissions.READ_WRITE`），将读取页添加到 local cache 中。
1. 调用 `BTreeFile.findLeafPage()` 查找应插入的叶子节点
2. 调用 `BTreeLeafPage.insertTuple()` 进行插入
3. 如果叶子节点已满，需要递归对节点进行分裂后再进行插入
   - `BTreeLeafPage splitLeafPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage page, Field field);`
		- 分裂叶子节点，返回分裂后应插入的叶子节点
		- 调用 `BTreeFile.getParentWithEmptySlots()`，将新的 entry 插入到父节点，如果父节点已满，递归分裂父节点
	- `BTreeInternalPage splitInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page, Field field);`
		- 分裂内部节点，返回分裂后应插入的内部节点
		- 调用 `BTreeFile.getParentWithEmptySlots()`，将新的 entry 插入到父节点，如果父节点已满，递归分裂父节点
	- `BTreeInternalPage getParentWithEmptySlots(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePageId parentId, Field field);`
		- 返回父节点，用于在父节点中插入项
		- 如果父节点已满，调用 `BTreeFile.splitInternalPage()` 分裂父节点，返回分裂后应插入的父节点
  
## Deletion
删除后，如果节点容量小于最小容量，重新分配/合并。
从左/右节点处重新分配 tuple/entry，或与左/右节点合并。
- `void stealFromLeafPage(BTreeLeafPage page, BTreeLeafPage sibling, BTreeInternalPage parent, BTreeEntry entry, boolean isRightSibling);`
	- 从左/右叶子节点处重新分配 tuple
	- 修改 parent 的 key
- `void stealFromLeftInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage leftSibling, BTreeInternalPage parent, BTreeEntry parentEntry);`
	- 从左内部节点处重新分配 entry
	- 将 parent 的 key 移下来，将左内部节点的 entry 移上去
- `void stealFromRightInternalPage(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage page, BTreeInternalPage rightSibling, BTreeInternalPage parent, BTreeEntry parentEntry);`
	- 从右内部节点处重新分配 entry
	- 将 parent 的 key 移下来，将右内部节点的 entry 移上去
- `void mergeLeafPages(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeLeafPage leftPage, BTreeLeafPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry);`
	- 合并叶子节点
	- 调用 `BTreeFile.deleteParentEntry()`，从 parent 中删除 parentEntry，如果 parent 容量小于最小容量，递归重新分配/合并 parent
- `void mergeInternalPages(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreeInternalPage leftPage, BTreeInternalPage rightPage, BTreeInternalPage parent, BTreeEntry parentEntry);`
	- 合并内部节点
	- 调用 `BTreeFile.deleteParentEntry()`，从 parent 中删除 parentEntry，如果 parent 容量小于最小容量，递归重新分配/合并 parent
- `void deleteParentEntry(TransactionId tid, HashMap<PageId, Page> dirtypages, BTreePage leftPage, BTreeInternalPage parent, BTreeEntry parentEntry);`
	- 调用 `BTreeInternalPage.deleteKeyAndRightChild()` 删除 entry
	- 如果删除后 parent 的容量小于最小容量，重新分配/合并 parent

# Time Spent

2 天。


