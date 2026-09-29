// Command grid_strategy places and refreshes a two-sided limit grid through
// the longtrader Go SDK.
//
// The point of this example is the *lifecycle*, not the grid maths. Every
// concept a ported strategy needs is visible in one place:
//
//   - session.Attach with a terminal API token (the only credential system),
//   - a negotiated heartbeat feeding the session lease watchdog (kill switch),
//   - ReconcileState before trading: the ATTACHED -> SYNCING -> ACTIVE gate,
//     after which orders are admitted,
//   - RegisterStrategy so the host can report status and counters,
//   - order submission carrying the session id, which is what lets the host
//     scope the kill-switch to *this* strategy's orders,
//   - StopStrategy(cancelOpenOrders) on shutdown, the same RPC the host uses
//     to guarantee nothing is left resting.
//
// Usage:
//
//	go run ./examples --base-url http://127.0.0.1:9000 \
//	    --token "$LONGTRADER_TOKEN" --iterations 3
package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
	"github.com/longcipher/longtrader/sdks/go/session"
)

// options is the parsed command line.
type options struct {
	baseURL         string
	token           string
	exchangeID      string
	symbol          string
	levels          int
	stepPct         float64
	amount          string
	refreshSecs     float64
	iterations      int
	leaseTimeoutSec int
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		// The flag package already printed the usage text.
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "grid: %v\n", err)
		os.Exit(2)
	}
	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "grid: %v\n", err)
		os.Exit(1)
	}
}

// parseFlags parses the command line, returning flag.ErrHelp when the user
// asked for --help (after the usage text has been printed).
func parseFlags(args []string) (options, error) {
	var opts options
	fs := flag.NewFlagSet("grid_strategy", flag.ContinueOnError)
	fs.StringVar(&opts.baseURL, "base-url", "", "worker control plane URL (required)")
	fs.StringVar(&opts.token, "token", "", "terminal API token")
	fs.StringVar(&opts.exchangeID, "exchange-id", "", "registered exchange instance id")
	fs.StringVar(&opts.symbol, "symbol", "BTC/USDT", "instrument to grid")
	fs.IntVar(&opts.levels, "levels", 3, "rungs per side")
	fs.Float64Var(&opts.stepPct, "step-pct", 0.1, "spacing between rungs, percent")
	fs.StringVar(&opts.amount, "amount", "0.001", "order size per rung, as a decimal string")
	fs.Float64Var(&opts.refreshSecs, "refresh-secs", 30, "seconds between grid refreshes")
	fs.IntVar(&opts.iterations, "iterations", 0, "grid refreshes before exiting; 0 runs until interrupted")
	fs.IntVar(&opts.leaseTimeoutSec, "lease-timeout-secs", 0, "kill-switch lease budget; 0 keeps the server default (3x heartbeat)")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	return opts, nil
}

func run(opts options) error {
	if opts.baseURL == "" {
		// Repeat the error through the flag package so the user sees the same
		// hint the parser would have given.
		return errors.New("--base-url is required (see --help)")
	}
	if opts.levels < 1 {
		return fmt.Errorf("--levels must be at least 1, got %d", opts.levels)
	}
	if opts.stepPct <= 0 {
		return fmt.Errorf("--step-pct must be positive, got %v", opts.stepPct)
	}
	if _, err := contract.ParseDecimal(opts.amount); err != nil {
		return fmt.Errorf("--amount: %w", err)
	}

	// Ctrl-C cancels the context, which unwinds the heartbeat, the watchdog
	// and any open stream, and then still runs the shutdown path.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	sess, err := session.Attach(ctx, opts.baseURL, opts.token, buildPolicy(opts), "")
	if err != nil {
		return fmt.Errorf("attach: %w", err)
	}
	defer sess.Close()
	fmt.Printf("attached session=%s heartbeat_ms=%d state=%s capabilities=%s\n",
		sess.SessionID(), sess.HeartbeatIntervalMS(), sess.State(), strings.Join(sess.Capabilities(), ","))

	// Register before trading so host-side status and counters cover the run.
	strategyID, err := sess.RegisterStrategy(ctx, "go_grid", map[string]string{
		"symbol":   opts.symbol,
		"levels":   strconv.Itoa(opts.levels),
		"step_pct": strconv.FormatFloat(opts.stepPct, 'f', -1, 64),
		"amount":   opts.amount,
	})
	if err != nil {
		return fmt.Errorf("register strategy: %w", err)
	}
	fmt.Printf("registered strategy=%s\n", strategyID)

	// The heartbeat feeds the host's lease watchdog; the local watchdog
	// mirrors it so this process stops trading even when the host is gone.
	// They are independent tiers, so either order works -- but starting the
	// watchdog after the heartbeat is what the example has always done, and
	// Started is checked so a silently-unsuppressed tier is not mistaken for
	// a running one.
	sess.StartHeartbeat(ctx)
	watchdog := sess.SpawnLeaseWatchdog(ctx, leaseBudget(opts))
	if !watchdog.Started {
		return errors.New("spawn lease watchdog: the session already runs one, or it is already stopped")
	}

	defer shutdown(sess, ctx)

	// Recovery gate: the authoritative snapshot before any submission.
	snapshot, err := sess.ReconcileState(ctx)
	if err != nil {
		return fmt.Errorf("reconcile: %w", err)
	}
	fmt.Printf("reconciled seq=%d balances=%d positions=%d open_orders=%d state=%s\n",
		snapshot.SnapshotSequence, len(snapshot.Balances), len(snapshot.Positions), len(snapshot.OpenOrders), sess.State())
	for _, balance := range snapshot.Balances {
		fmt.Printf("  balance %s free=%s used=%s\n", balance.Currency, balance.Free, balance.Total)
	}
	for _, position := range snapshot.Positions {
		fmt.Printf("  position %s %s entry=%s\n", position.Symbol, position.Side, position.EntryPrice)
	}
	for _, order := range snapshot.OpenOrders {
		fmt.Printf("  open order %s %s %s @ %s\n", order.ID, order.Symbol, order.Side, order.Price)
	}

	var liveIDs []string
	for iteration := 0; opts.iterations == 0 || iteration < opts.iterations; iteration++ {
		if sess.State() != session.StateActive {
			fmt.Printf("[grid] session is %s; stopping\n", sess.State())
			break
		}
		liveIDs, err = refreshGrid(ctx, sess, opts, liveIDs)
		if err != nil {
			return err
		}
		if opts.iterations > 0 && iteration+1 < opts.iterations {
			select {
			case <-ctx.Done():
				fmt.Println("interrupted")
				return nil
			case <-time.After(time.Duration(opts.refreshSecs * float64(time.Second))):
			}
		}
	}
	return nil
}

// buildPolicy is the kill-switch policy: cancel this session's orders on lease
// loss. A zero lease timeout leaves the server default (3x heartbeat).
func buildPolicy(opts options) *contract.KillSwitchPolicy {
	policy := &contract.KillSwitchPolicy{Scope: contract.KillSwitchScopeSessionOrders}
	if opts.leaseTimeoutSec > 0 {
		policy.LeaseTimeout = contract.DurationFrom(time.Duration(opts.leaseTimeoutSec) * time.Second)
	}
	return policy
}

// leaseBudget is the local watchdog's budget: the same --lease-timeout-secs the
// kill-switch policy carries, or 0 for the SDK's own three-heartbeat default.
//
// The two are the same number on purpose. The host's watchdog and this
// process's watchdog are the same tier seen from two sides, and a local budget
// stricter than the host's trips the strategy while the host still believes the
// lease is alive; a looser one leaves orders resting after the host gave up.
func leaseBudget(opts options) time.Duration {
	if opts.leaseTimeoutSec <= 0 {
		return 0
	}
	return time.Duration(opts.leaseTimeoutSec) * time.Second
}

// refreshGrid cancels the previous rungs, then batch-places a fresh grid
// around the mid price.
//
// Every RPC pins the same --exchange-id the mid price was read from, so the
// grid is priced and traded on one backend. WithExchangeID("") leaves the field
// unset and the host applies its own default, which is the pre-existing
// behaviour when the flag is omitted.
func refreshGrid(ctx context.Context, sess *session.Session, opts options, liveIDs []string) ([]string, error) {
	mid, err := fetchMid(ctx, sess, opts.symbol, opts.exchangeID)
	if err != nil {
		return nil, fmt.Errorf("fetching %s mid: %w", opts.symbol, err)
	}
	fmt.Printf("[grid] mid=%.2f\n", mid)
	venue := session.WithExchangeID(opts.exchangeID)

	// Cancel the previous rungs first, so the grid never doubles up.
	for _, orderID := range liveIDs {
		if _, err := sess.CancelOrder(ctx, orderID, opts.symbol, venue); err != nil {
			// One rung failing to cancel is not fatal: the batch below re-prices
			// the book, and the kill-switch is the final backstop.
			fmt.Printf("[grid] cancel %s failed: %v\n", orderID, err)
		}
	}

	specs := make([]session.OrderSpec, 0, 2*opts.levels)
	for level := 1; level <= opts.levels; level++ {
		step := opts.stepPct / 100.0 * float64(level)
		for _, rung := range []struct {
			side  contract.OrderSide
			price float64
		}{
			{contract.OrderSideBuy, mid * (1 - step)},
			{contract.OrderSideSell, mid * (1 + step)},
		} {
			price := formatPrice(rung.price)
			specs = append(specs, session.OrderSpec{
				Symbol:        opts.symbol,
				Amount:        opts.amount,
				Price:         &price,
				Side:          rung.side,
				Type:          contract.OrderTypeLimit,
				TimeInForce:   contract.TimeInForceGTC,
				ClientOrderID: clientOrderID(),
				PostOnly:      true,
			})
		}
	}

	// One batch, one gate check: the host rejects the whole batch if the
	// session is not ACTIVE, so a grid is never half-placed.
	orders, err := sess.CreateOrders(ctx, specs, venue)
	if err != nil {
		return nil, fmt.Errorf("placing grid: %w", err)
	}
	placed := make([]string, 0, len(orders))
	for _, order := range orders {
		placed = append(placed, order.ID)
	}
	fmt.Printf("[grid] placed %d rungs\n", len(placed))
	return placed, nil
}

// fetchMid polls one ticker and computes a mid price, falling back to the last
// trade and then to the close.
func fetchMid(ctx context.Context, sess *session.Session, symbol, exchangeID string) (float64, error) {
	ticker, err := sess.FetchTicker(ctx, symbol, session.WithExchangeID(exchangeID))
	if err != nil {
		return 0, err
	}
	bid, bidOK := decimalToFloat(ticker.Bid)
	ask, askOK := decimalToFloat(ticker.Ask)
	if bidOK && askOK && bid > 0 && ask > 0 {
		return (bid + ask) / 2, nil
	}
	if last, ok := decimalToFloat(ticker.Last); ok && last > 0 {
		return last, nil
	}
	if close, ok := decimalToFloat(ticker.Close); ok && close > 0 {
		return close, nil
	}
	return 0, errors.New("ticker has no usable price")
}

// decimalToFloat reads a contract decimal in *both* representations.
//
// The wire type carries two forms and a writer populates only one: the
// unscaled/scale fast path, or the human-readable raw_str fallback. The mock
// venue fills only the numeric pair, so a reader that consults raw_str alone
// sees no price at all -- and one that consults the numeric pair alone cannot
// read a 96-bit mantissa the host sent as text.
func decimalToFloat(d contract.Decimal) (float64, bool) {
	if d.RawStr != "" {
		value, err := strconv.ParseFloat(d.RawStr, 64)
		return value, err == nil
	}
	if d.Unscaled == 0 {
		return 0, true
	}
	if d.Scale < 0 || d.Scale > 18 {
		return 0, false
	}
	return float64(d.Unscaled) / math.Pow(10, float64(d.Scale)), true
}

// formatPrice renders a price without trailing zeros, so the order message
// stays readable in a venue log.
func formatPrice(price float64) string {
	text := strconv.FormatFloat(price, 'f', 8, 64)
	if strings.Contains(text, ".") {
		text = strings.TrimRight(text, "0")
		text = strings.TrimSuffix(text, ".")
	}
	return text
}

// clientOrderID returns a 26-character uppercase id, ULID-shaped.
//
// The contract only demands uniqueness per account: the backend dedupes on
// client_order_id, so reusing an id across retries is safe and required. Swap
// in a real time-ordered ULID generator for production -- this one is random
// so two strategies started in the same nanosecond cannot collide.
func clientOrderID() string {
	// Crockford base32, minus I, L, O and U: no confusable glyphs in a log.
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	raw := make([]byte, 26)
	if _, err := rand.Read(raw); err != nil {
		// crypto/rand does not fail in practice; fall back to the clock rather
		// than submit a constant id, which the backend would dedupe away.
		seed := uint64(time.Now().UnixNano())
		for i := range raw {
			seed = seed*6364136223846793005 + 1442695040888963407
			raw[i] = byte(seed >> 33)
		}
	}
	for i, b := range raw {
		raw[i] = alphabet[int(b)%len(alphabet)]
	}
	return string(raw)
}

// shutdown lets the host cancel exactly this session's resting orders. This is
// the same path the kill-switch uses, so a crash-looping strategy cannot leave
// ladders behind. It is best-effort: the session may already be terminal.
func shutdown(sess *session.Session, ctx context.Context) {
	// The context may already be cancelled (Ctrl-C), so give the RPC its own
	// short-lived window.
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := sess.StopStrategy(stopCtx, true); err != nil {
		fmt.Printf("stop_strategy failed: %v\n", err)
		return
	}
	fmt.Printf("stopped final_state=%s\n", sess.State())
}
