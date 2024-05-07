# Design Choices

## Operation

### Operator extends DbIterator
- `fetch_next` ：返回下一个满足 operator 条件的 tuple，由 operator 的子类实现
- `next`
- `has_nesxt`

## Filter extends Operator

## Join extends Operator
nested loop join
- `Tuple tuple1` 指向 outer table 中正在处理的 tuple
- `Tuple tuple2` 指向 inner table 中正在处理的 tuple

## HashEquiJoin extends Operator
hash join
- `HashMap<Field, List<Tuple>> hashMap` ：一个可以将同一个 key 映射到多个 value 的哈希表
- `Tuple tuple1` 指向 outer table 中正在处理的 tuple
- `Iterator<Tuple> listIt` 指向  `hashMap` 中与 `tuple1` 对应的 `List` 中正在处理的 tuple

## Aggregation

### Aggregate extends Operator
负责整个 aggregation 计算。
循环调用 `Aggregator.mergeTupleIntoGroup()` 将 `child` 返回的下一个 tuple 融入已有的 aggregation 计算中。

### Aggregator
负责将一个新的 tuple 融入已有的 aggregation 计算中。
- `HashMap<Field, IntField> groupToValue` 保存每一组的 value
- `HashMap<Field, IntField> groupToCount` 保存每一组的 count
- `List<Tuple>` 将 `groupToValue` 中的 group 和 value 组合为 tuple，供 `DbIterator` 遍历

## Insertion and Deletion

### BufferPool
- `void insertTuple(TransactionId tid, int tableId, Tuple t)` ：向 table 中插入 tuple，将所有 dirtyPages 标记为脏并将其放入 buffer 中
- `void deleteTuple(TransactionId tid, int tableId, Tuple t)` ：从 table 中删除 tuple，将所有 dirtyPages 标记为脏并将其放入 buffer 中

### HeapFile
- `ArrayList<Page> insertTuple(Tuple t)`：找到第一个有空 slot 的 page，将 tuple 插入
- `ArrayList<Page> deleteTuple(Tuple t)`：根据 tuple 的 recordId 找到 page 并从中删除 tuple

### HeapPage
- `void insertTuple(Tuple t)`：找到第一个空 slot，将 tuple 插入并更新 tuple 的 recordId
- `void deleteTuple(Tuple t)`：根据 tuple 的 recordId 找到 slot 从中删除 tuple


