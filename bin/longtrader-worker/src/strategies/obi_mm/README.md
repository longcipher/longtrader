# obi_mm

Order Book Imbalance (OBI) market maker.

## Model

Predicts short-term price movement from the relative pressure of bid vs ask
sizes at the top of the book:

```
OBI    = (bid_size - ask_size) / (bid_size + ask_size)    in [-1, 1]
fair   = mid + alpha * OBI * mid
bid    = fair * (1 - half_spread)
ask    = fair * (1 + half_spread)
```

`alpha` is the signal weight -- how aggressively the strategy tilts on the
imbalance. A positive OBI (more bid depth) signals upward pressure, so the
fair price moves up and the bid tightens to capture the expected rise before
the ask profits on the reversion.

The favoured side receives a quote-size boost (`size_boost`) when the OBI
signal is strong and aligned with that side.

## Parameters

| Key              | Default | Description                                    |
|------------------|---------|------------------------------------------------|
| `half_spread`    | 0.0005  | Half-spread as a fraction of fair price.        |
| `signal_weight`  | 0.001   | Fair-price tilt per unit OBI.                   |
| `qty`            | 0.001   | Quote quantity per side.                       |
| `depth`          | 5       | Book levels from which OBI is accumulated.      |
| `size_boost`     | 2.0     | Quote-size multiplier on the favoured side.     |
| `boost_threshold` | 0.3 | Minimum |

## When to use

Liquid, small-tick markets where order-flow imbalance has predictive power
for short-term price direction -- high-crypto spot and perpetuals.
