> [English](README.md) | **中文**

# sentinel — 连接健康哨兵

周期探测 ticker+account；连续 `max_consecutive_failures` 次失败触发 kill-switch（全撤单），恢复后计数归零。
