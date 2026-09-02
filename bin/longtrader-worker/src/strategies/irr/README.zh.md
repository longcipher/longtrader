> [English](README.md) | **中文**

# irr — 预测收益率入场

读取外部信号文件（最后一行为预测收益率），高于 `entry_threshold` 开多、低于负阈值平仓。模型推理留在交易进程之外。

参数：`signal_file`、`entry_threshold`、`qty`。
