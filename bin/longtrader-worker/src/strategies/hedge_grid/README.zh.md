> [English](README.md) | **中文**

# hedge_grid — 对冲网格

primary 所网格 + 每次探测到成交失衡后在 hedge 所反向市价对冲，净敞口趋零。

参数：网格四件套（lower/upper/num_levels/qty_per_level）+ 双 venue 引用。
