# queue_mm

Queue-based market maker for large-tick assets.

## Model

When the tick size is wide relative to the spread (large-tick regime), the
first queue dominates price formation. The strategy tracks two signals:

```
pressure   = log(Σ bid_qty / Σ ask_qty)
impulse    = Σ(buy_volume) − Σ(sell_volume)     (signed, rolling window)
signal     = w₁ · pressure + w₂ · normalise(impulse)
fair      += α · signal · mid
```

A wide-book (more bid depth) or strong buy impulse tilts the fair value up;
the ask tightens to capture the expected move, the bid widens to avoid
adverse selection. Spreads widen linearly with `|signal|` (parameter
`spread_widen`).

The strategy does not place orders deeper than `max_depth` ticks from mid.

## Parameters

| Key             | Default | Description                                   |
|-----------------|---------|-----------------------------------------------|
| `half_spread`   | 0.0005  | Half-spread baseline as fraction.             |
| `qty`           | 0.001   | Quote base quantity.                          |
| `depth`         | 5       | Look depth for book pressure (levels).        |
| `impulse_window`| 20      | Trade-impulse rolling window (ticks).         |
| `w_pressure`    | 0.5     | Signal weight `w₁` on book pressure.          |
| `w_impulse`     | 0.5     | Signal weight `w₂` on trade impulse.          |
| `alpha`         | 0.001   | Aggressiveness factor α.                      |
| `spread_widen` | 0.001 | Half-spread widening per unit |

## When to use

Large-tick assets (low-price equities, some FX pairs, small-cap tokens)
where queue position and trade flow drive short-term price formation more
than the mid-price alone.
