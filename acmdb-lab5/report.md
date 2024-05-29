# Design Choices
## Lock

一个锁
- `tid`：持有该锁的 `Transaction` 的 `TransactionId`
- `pid`：持有该锁的 `Page` 的 `PageId`
- `isExclusive`：该锁是否为独享锁

每个 `Transaction` 的每个 `Page` 只可以有一个锁。

## Locks

所有锁
- `tidToLock`：`Transaction` 与锁的对应关系
- `pidToLock`：`Page` 与锁的对应关系
- `lock`：加锁
  - 如果这个 `Transaction` 的这个 `Page` 已经有了一个锁
    - 如果这个锁是独享锁，则获得该锁
    - 如果这个锁是共享锁，且要申请的锁也是共享锁，则获得该锁
    - 如果这个锁是共享锁，但要申请的锁是独享锁
      - 如果这个 `Page` 只有这一个锁，则升级并获得该锁
      - 如果这个 `Page` 不只有一个锁，则阻塞
  - 如果这个 `Transaction` 的这个 `Page` 还没有锁
   - 如果申请的锁是共享锁，但这个 `Page` 有独享锁，则阻塞
   - 如果申请的锁是独享锁，但这个 `Page` 有锁，则阻塞
   - 其他情况，则构造一个新锁并获得该锁
- `unlock`：释放锁
- `getLockedPages`：获取某个 `Transaction` 锁住的所有页面
- `unlockPages`：释放某个 `Transaction` 持有的所有锁
