> **English** | [中文](README.zh.md)

# irr — Predicted Yield Entry

Reads an external signal file (last line = predicted yield). Opens long when above `entry_threshold`, closes when below negative threshold. Model inference stays out of the trading process.

Params: `signal_file`, `entry_threshold`, `qty`.
