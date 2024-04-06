# Design decisions

- 添加 `HeapFileIterator` 类
  - 维护一个 `HeapPage` 类的迭代器 `it`
    - 如果 `HeapFileIterator` 没有 `open` ，或者 `it` 指向最后一个 `HeapPage` 的最后一个 `Tuple`，则 `next = null`
    - 否则，如果 `it` 指向当前 `HeapPage` 的最后一个 `Tuple`，则调用 `BufferPool.getPage()` 将下一页读取到内存中，`next` 指向下一页的第一个 `Tuple`
    - 否则， `next` 指向当前 `HeapPage` 的下一个 `Tuple`

# Time Spent on the Lab

3天
