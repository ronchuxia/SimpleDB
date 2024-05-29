# Design Choices
## TableStats
1. 统计 DbFile 中 Tuple 的个数，用于估计 SeqScan 的开销。
    - `double estimateScanCost()` ：估计顺序读取 DbFile 中所有 Tuple 的开销（`cost = numPages * ioCostPerPage`）

2. 为 DbFile 的每个 Field 构建一个 Histogram，用于估计 DbFile 经过某次 Filter 操作后的 cardinality（filter selectivity estimation）。
    - `double estimateSelectivity(int field, Predicate.Op op, Field constant)`：估计 `Predicate op` 的 selectivity（使用 histogram 进行估计）
    - `int estimateTableCardinality(double selectivityFactor)`：估计 DbFile 经过某次 Filter 操作后的 cardinality

## JoinOptimizer
1. 估计某次 Join 的开销和经过某次 Join 操作后的 cardinality。
    - `estimateJoinCost(LogicalJoinNode j, int card1, int card2, double cost1, double cost2)`：估计某次 join 操作的开销（`cost = scancost(t1) + ntups(t1) * scancost(t2) + ntups(t1) * ntups(t2)`）
    - `estimateJoinCardinality(LogicalJoinNode j, int card1, int card2, boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats)`：估计某次 join 操作后的 cardinality

2. 使用 Selinger 算法（动态规划）选择最佳的 join 顺序
    - `Vector<LogicalJoinNode> orderJoins(HashMap<String, TableStats> stats, HashMap<String, Double> filterSelectivities, boolean explain)`

# Time Spent
1 天。


