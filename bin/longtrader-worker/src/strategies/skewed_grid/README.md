# skewed_grid

Avellaneda–Stoikov inventory-risk skewed grid market maker.

## Model

Each tick, the grid is re-anchored to a **reservation price**:

```
R = S − q·γ·σ²·(T − t)
```

where `q` is signed inventory, `γ` is the risk-aversion coefficient, `σ` is
realised volatility, and `(T − t)` is the remaining time horizon. `num_levels`
buy grids are placed below `R` and `num_levels` sell grids above `R`, each
spaced by `grid_tick`.

As inventory grows, the whole ladder shifts: the ask side tightens
(encouraging sells) and the bid side widens (discouraging buys) — the
canonical AS control that keeps inventory bounded.

## Parameters

| Key             | Default | Description                                   |
|-----------------|---------|-----------------------------------------------|
| `grid_tick`     | —       | Grid spacing in price units (e.g. `1`).        |
| `num_levels`    | 5       | Levels on each side of the reservation price.  |
| `qty_per_level` | —       | Order quantity per level.                     |
| `gamma`         | 0.1     | Risk-aversion coefficient γ.                  |
| `sigma`         | 1.0     | Volatility σ (fallback before EMA warms up).  |
| `time_horizon`  | 1.0     | Remaining horizon in matching units.           |
| `vol_window`    | 20      | Rolling window for realised vol estimation.    |

## When to use

Inventory-sensitive market making where the spread should adapt to the
trader's current position — spot-perp basis, derivatives hedging, and any
unidirectional risk that a flat grid cannot control.
