package session

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/longcipher/longtrader/sdks/go/contract"
)

// leaseHost answers with a fast negotiated heartbeat and a host that has gone
// away for KeepAlive, which is the situation the lease tier exists for. The
// StopStrategy call is recorded and refused, exactly as it would be once the
// host has already tripped its own kill-switch.
func leaseHost(t *testing.T, heartbeatMS uint32) *fakeHost {
	t.Helper()
	return defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{
				SessionID:           "sess-1",
				HeartbeatIntervalMS: heartbeatMS,
				Capabilities:        []string{"reconcile"},
			})
		},
		"KeepAlive": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeError(w, http.StatusServiceUnavailable, "unavailable", "heartbeat rejected")
		},
		"StopStrategy": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			// The host is gone, which is why the lease lapsed. The trip is
			// local; the host remains the authority for the real cancellation.
			h.writeError(w, http.StatusServiceUnavailable, "unavailable", "session lease expired")
		},
	})
}

// The lease tier must fire when KeepAlive stops succeeding -- and, crucially,
// must still fire *after* a failed KeepAlive. Gating the check on success (the
// bug this guards) leaves the guard dead in the one case it exists for.
// The WaitGroup counter must be incremented under the same lock that guards
// the started flag. Otherwise Stop can pass Wait with a zero counter and
// return before a freshly spawned loop has registered itself, which is
// unsound even though the loop self-terminates: Stop is documented to wait,
// and a caller that closed over anything the loop touches would be racing.
//
// Each round uses a fresh session because Stop is one-shot by design.
func TestStopCannotObserveAnUnregisteredLoop(t *testing.T) {
	host := leaseHost(t, 10)
	for i := 0; i < 100; i++ {
		s, err := Attach(context.Background(), host.url(), "tok", nil, "")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		if !s.StartHeartbeat(context.Background()) {
			t.Fatalf("iteration %d: the heartbeat must start", i)
		}
		s.Stop()
		if got := s.backgroundLoops(); got != 0 {
			t.Fatalf("iteration %d: %d loops live after Stop", i, got)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
	}
}

// One Stop must reap every tier, including when the loops were registered from
// different goroutines: the shared context is what makes the second
// registration join the first one's cancellation.
func TestStopReapsLoopsStartedConcurrently(t *testing.T) {
	host := leaseHost(t, 10)
	for i := 0; i < 50; i++ {
		s, err := Attach(context.Background(), host.url(), "tok", nil, "")
		if err != nil {
			t.Fatalf("Attach: %v", err)
		}
		var wg sync.WaitGroup
		results := make([]bool, 2)
		wg.Add(2)
		go func() {
			defer wg.Done()
			results[0] = s.StartHeartbeat(context.Background())
		}()
		go func() {
			defer wg.Done()
			results[1] = s.SpawnLeaseWatchdog(context.Background(), time.Minute).Started
		}()
		wg.Wait()
		// At most one of each tier may start, whatever the interleaving.
		if !results[0] || !results[1] {
			t.Fatalf("iteration %d: both tiers must start, got %v", i, results)
		}
		s.Stop()
		if got := s.backgroundLoops(); got != 0 {
			t.Fatalf("iteration %d: %d loops live after Stop", i, got)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
	}
}

func TestStartHeartbeatTripsTheLeaseAfterFailedKeepAlives(t *testing.T) {
	host := leaseHost(t, 20)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	s.StartHeartbeat(context.Background())
	// The default budget is 3 heartbeats with a 1.5s floor, so allow for it.
	waitFor(t, "the lease to trip", 5*time.Second, func() bool { return s.State() == StateKillSwitchTripped })
	waitFor(t, "the best-effort StopStrategy", 2*time.Second, func() bool { return host.countCalls("StopStrategy") > 0 })

	if host.countCalls("KeepAlive") == 0 {
		t.Error("the heartbeat must have attempted KeepAlive before the lease lapsed")
	}
	call, _ := host.lastCall("StopStrategy")
	var req contract.StopStrategyRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding StopStrategyRequest: %v", err)
	}
	if !req.CancelOpenOrders {
		t.Error("the lease trip must ask the host to cancel this session's open orders")
	}
	if s.CanTrade() {
		t.Error("a tripped session must not be able to trade")
	}
	if _, err := s.CreateOrder(context.Background(), OrderSpec{Symbol: "BTC/USDT", Amount: "0.001"}); err == nil {
		t.Error("order submission must be refused after a lease trip")
	}
	// The loop stops after tripping, so no further heartbeats are sent.
	settled := host.countCalls("KeepAlive")
	time.Sleep(100 * time.Millisecond)
	if got := host.countCalls("KeepAlive"); got > settled+1 {
		t.Errorf("the heartbeat kept running after the lease tripped: %d -> %d", settled, got)
	}
}

// The explicit watchdog is a separate tier: it guards a strategy that drives
// its own heartbeat, or one that has lost the host entirely.
func TestSpawnLeaseWatchdogTripsOnItsOwn(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	// An explicit budget keeps the test fast and deterministic; the default
	// three-heartbeat floor is exercised by TestStartHeartbeatTripsTheLease.
	watchdog := s.SpawnLeaseWatchdog(context.Background(), 60*time.Millisecond)
	if !watchdog.Started {
		t.Fatal("the watchdog must report that it started")
	}
	select {
	case <-watchdog.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("the watchdog did not stop")
	}
	if got := s.State(); got != StateKillSwitchTripped {
		t.Errorf("state = %q, want %q", got, StateKillSwitchTripped)
	}
	waitFor(t, "the best-effort StopStrategy", 2*time.Second, func() bool { return host.countCalls("StopStrategy") > 0 })
	call, _ := host.lastCall("StopStrategy")
	var req contract.StopStrategyRequest
	if err := req.Unmarshal(call.Body); err != nil {
		t.Fatalf("decoding StopStrategyRequest: %v", err)
	}
	if !req.CancelOpenOrders || req.SessionID != "sess-1" {
		t.Errorf("StopStrategyRequest = %+v", req)
	}
}

// A healthy heartbeat must keep the lease alive indefinitely; a watchdog that
// trips on a working session is worse than no watchdog at all.
//
// The lease budget is deliberately generous relative to the 10ms heartbeat, and
// the assertion window deliberately spans two whole budgets. A 60ms budget
// against a 10ms heartbeat leaves six intervals of slack, which is not enough:
// under CPU contention a 10ms timer plus an httptest round-trip can exceed it,
// and this test then failed intermittently for reasons that have nothing to do
// with the behaviour it is checking. The trip tests below keep tight budgets on
// purpose, since firing is exactly what they are asserting.
func TestLeaseWatchdogStaysQuietWhileHeartbeatsSucceed(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 10})
		},
		"KeepAlive": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			// A host may re-negotiate a longer interval; echo the same one here
			// so the test drives several fast ticks.
			h.writeProto(w, &contract.KeepAliveResponse{ServerTimeNS: 1, HeartbeatIntervalMS: 10})
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	s.StartHeartbeat(context.Background())
	const leaseBudget = 1000 * time.Millisecond
	s.SpawnLeaseWatchdog(context.Background(), leaseBudget)
	// Several heartbeat intervals, then two full lease budgets of observation.
	// If the heartbeat refreshes the lease, the watchdog cannot trip no matter
	// how long the window is; if it does not, the first budget is enough.
	waitFor(t, "several successful heartbeats", 3*time.Second, func() bool { return host.countCalls("KeepAlive") > 3 })
	time.Sleep(2 * leaseBudget)
	if got := s.State(); got != StateActive {
		t.Errorf("state = %q, want %q: a live heartbeat must keep the lease", got, StateActive)
	}
	if n := host.countCalls("StopStrategy"); n != 0 {
		t.Errorf("StopStrategy was called %d times while the lease was healthy", n)
	}
}

// The trip is latched: a later watchdog must not re-trip or re-notify.
func TestLeaseTripIsLatched(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	s.setState(StateActive)

	s.SpawnLeaseWatchdog(context.Background(), 40*time.Millisecond)
	waitFor(t, "the lease to trip", 3*time.Second, func() bool { return s.State() == StateKillSwitchTripped })
	before := host.countCalls("StopStrategy")
	for i := 0; i < 3; i++ {
		if s.checkLease(context.Background(), 10*time.Millisecond) {
			t.Fatal("checkLease must report no further trip once latched")
		}
	}
	if after := host.countCalls("StopStrategy"); after != before {
		t.Errorf("StopStrategy ran %d extra times after latching", after-before)
	}
}

func TestLeaseBudgetDefaultsToThreeHeartbeats(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 900})
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if got := s.leaseBudget(0); got != 2700*time.Millisecond {
		t.Errorf("leaseBudget(0) = %v, want 2.7s (3x heartbeat)", got)
	}
	if got := s.leaseBudget(123 * time.Millisecond); got != 123*time.Millisecond {
		t.Errorf("an explicit budget must win, got %v", got)
	}
	// A tiny heartbeat must not produce a tiny lease.
	host2 := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 10})
		},
	})
	s2, err := Attach(context.Background(), host2.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s2.Close()
	if got := s2.leaseBudget(0); got != defaultLeaseFloor {
		t.Errorf("leaseBudget(0) = %v, want the %v floor", got, defaultLeaseFloor)
	}
}

func TestHeartbeatIntervalIsFloored(t *testing.T) {
	host := leaseHost(t, 1)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if got := s.heartbeatInterval(); got != heartbeatFloor {
		t.Errorf("heartbeatInterval = %v, want the %v floor", got, heartbeatFloor)
	}
}

// The host may re-negotiate a longer interval mid-session; the next tick must
// follow the new value, and a zero in the reply must not reset it to the floor.
func TestKeepAliveRenegotiatesTheInterval(t *testing.T) {
	host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
		"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 40})
		},
		"KeepAlive": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
			h.writeProto(w, &contract.KeepAliveResponse{HeartbeatIntervalMS: 2000})
		},
	})
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()
	if _, err := s.KeepAlive(context.Background()); err != nil {
		t.Fatalf("KeepAlive: %v", err)
	}
	if got := s.HeartbeatIntervalMS(); got != 2000 {
		t.Errorf("HeartbeatIntervalMS = %d, want the re-negotiated 2000", got)
	}
	if got := s.heartbeatInterval(); got != 2*time.Second {
		t.Errorf("heartbeatInterval = %v, want 2s", got)
	}
}

// A terminal session must not be dragged into a kill-switch trip: a graceful
// shutdown is not a lease failure.
func TestCheckLeaseIgnoresTerminalStates(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	s.setState(StateGracefulShutdown)
	if s.checkLease(context.Background(), time.Nanosecond) {
		t.Error("a graceful shutdown must not trip the lease")
	}
	if got := s.State(); got != StateGracefulShutdown {
		t.Errorf("state = %q, want GRACEFUL_SHUTDOWN", got)
	}
	if n := host.countCalls("StopStrategy"); n != 0 {
		t.Errorf("StopStrategy ran %d times for a terminal session", n)
	}
}

// StartHeartbeat and SpawnLeaseWatchdog are two independent safety tiers (the
// Python SDK runs them as two independent threads), so either may be started
// first and neither may be suppressed by the other. The shared single-slot
// latch this replaces let whichever ran second fail to spawn, and in the order
// the example calls them that left the heartbeat unsent -- so the host's lease
// watchdog killed a session that looked healthy.
func TestBothLeaseTiersStartInEitherOrder(t *testing.T) {
	for _, order := range []string{"heartbeat-first", "watchdog-first"} {
		t.Run(order, func(t *testing.T) {
			host := defaultHost(t, map[string]func(*fakeHost, http.ResponseWriter, recordedCall){
				"AttachSession": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
					h.writeProto(w, &contract.AttachSessionResponse{SessionID: "sess-1", HeartbeatIntervalMS: 10})
				},
				"KeepAlive": func(h *fakeHost, w http.ResponseWriter, call recordedCall) {
					h.writeProto(w, &contract.KeepAliveResponse{HeartbeatIntervalMS: 10})
				},
			})
			s, err := Attach(context.Background(), host.url(), "tok", nil, "")
			if err != nil {
				t.Fatalf("Attach: %v", err)
			}
			defer s.Close()

			var heartbeatStarted bool
			var watchdog LeaseWatchdog
			if order == "heartbeat-first" {
				heartbeatStarted = s.StartHeartbeat(context.Background())
				watchdog = s.SpawnLeaseWatchdog(context.Background(), time.Minute)
			} else {
				watchdog = s.SpawnLeaseWatchdog(context.Background(), time.Minute)
				heartbeatStarted = s.StartHeartbeat(context.Background())
			}
			if !heartbeatStarted {
				t.Error("StartHeartbeat reported that it did not start")
			}
			if !watchdog.Started {
				t.Error("SpawnLeaseWatchdog reported that it did not start")
			}
			if got := s.backgroundLoops(); got != 2 {
				t.Errorf("background loops = %d, want 2 (one per tier)", got)
			}
			// The consequence that matters: the heartbeat must actually be
			// sending, whatever order the tiers were started in.
			waitFor(t, "KeepAlive traffic", 3*time.Second, func() bool { return host.countCalls("KeepAlive") > 0 })
		})
	}
}

// A repeated call of the *same* tier is still rejected -- one session sends one
// heartbeat stream and runs one watchdog -- but rejecting it must be visible to
// the caller rather than silently returning a closed channel.
func TestRepeatedStartOfTheSameTierIsRejected(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	if !s.StartHeartbeat(context.Background()) {
		t.Fatal("the first StartHeartbeat must start")
	}
	if s.StartHeartbeat(context.Background()) {
		t.Error("a second StartHeartbeat must not spawn a second loop")
	}
	if first := s.SpawnLeaseWatchdog(context.Background(), time.Minute); !first.Started {
		t.Fatal("the first SpawnLeaseWatchdog must start")
	}
	if second := s.SpawnLeaseWatchdog(context.Background(), time.Minute); second.Started {
		t.Error("a second SpawnLeaseWatchdog must not spawn a second loop")
	}
	if got := s.backgroundLoops(); got != 2 {
		t.Errorf("background loops = %d, want 2", got)
	}
}

// "Started and finished" and "never started" must not look alike. A closed Done
// channel used to be returned for a watchdog that was silently never spawned,
// so a caller waiting on it read success for a loop that did not exist.
func TestWatchdogHandleDistinguishesNeverStarted(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	started := s.SpawnLeaseWatchdog(context.Background(), 40*time.Millisecond)
	if !started.Started {
		t.Fatal("the first watchdog must report that it started")
	}
	// This one is a trip: the host is unreachable, so the lease lapses.
	waitFor(t, "the lease to trip", 3*time.Second, func() bool { return s.State() == StateKillSwitchTripped })
	select {
	case <-started.Done:
	case <-time.After(3 * time.Second):
		t.Fatal("a started watchdog that finished must close Done")
	}
	// The session now runs no watchdog, so a second call does not start.
	never := s.SpawnLeaseWatchdog(context.Background(), time.Minute)
	if never.Started {
		t.Error("the second watchdog must report that it did not start")
	}
	select {
	case <-never.Done:
	default:
		t.Error("a watchdog that never started must not leave Done open; Started is the signal")
	}

	// A stopped session accepts no loop at all, so nothing outlives Stop.
	s.Stop()
	if after := s.SpawnLeaseWatchdog(context.Background(), time.Minute); after.Started {
		t.Error("a stopped session must not start a watchdog")
	}
	if s.StartHeartbeat(context.Background()) {
		t.Error("a stopped session must not start a heartbeat")
	}
	if got := s.backgroundLoops(); got != 0 {
		t.Errorf("background loops = %d, want 0", got)
	}
}

// Stop must reap both tiers: a watchdog that ignores its context would keep the
// WaitGroup (and therefore Stop) blocked forever.
func TestStopStopsBothLeaseTiers(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	if !s.StartHeartbeat(context.Background()) {
		t.Fatal("the heartbeat must start")
	}
	watchdog := s.SpawnLeaseWatchdog(context.Background(), time.Minute)
	if !watchdog.Started {
		t.Fatal("the watchdog must start")
	}
	waitFor(t, "both loops to register", 2*time.Second, func() bool { return s.backgroundLoops() == 2 })

	s.Stop()
	if got := s.backgroundLoops(); got != 0 {
		t.Errorf("background loops = %d after Stop, want 0", got)
	}
	select {
	case <-watchdog.Done:
	case <-time.After(2 * time.Second):
		t.Error("Stop must close the watchdog's Done channel")
	}
	// A heartbeat that kept running would keep calling a host that is gone.
	settled := host.countCalls("KeepAlive")
	time.Sleep(50 * time.Millisecond)
	if got := host.countCalls("KeepAlive"); got > settled {
		t.Errorf("the heartbeat kept running after Stop: %d -> %d", settled, got)
	}
}

// The first tier to start owns the shared lifetime, so a later caller passing a
// different context gets a loop that its own cancellation will not stop. That is
// surprising enough to pin: it is documented on startBackground, and this is the
// test that makes the documentation trustworthy.
//
// StartHeartbeat(ctxOwner) then SpawnLeaseWatchdog(ctxOther) must therefore leave
// both loops running after ctxOther is cancelled -- only cancelling ctxOwner, or
// calling Stop, may take the watchdog down.
func TestFirstTierOwnsTheSharedLifetime(t *testing.T) {
	host := leaseHost(t, 10)
	s, err := Attach(context.Background(), host.url(), "tok", nil, "")
	if err != nil {
		t.Fatalf("Attach: %v", err)
	}
	defer s.Close()

	ctxOwner, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()
	ctxOther, cancelOther := context.WithCancel(context.Background())

	if !s.StartHeartbeat(ctxOwner) {
		t.Fatal("the heartbeat must start")
	}
	watchdog := s.SpawnLeaseWatchdog(ctxOther, time.Minute)
	if !watchdog.Started {
		t.Fatal("the watchdog must start")
	}
	waitFor(t, "both loops to register", 2*time.Second, func() bool { return s.backgroundLoops() == 2 })

	// Cancelling the second caller's context must not take the shared loops down.
	cancelOther()
	time.Sleep(100 * time.Millisecond)
	if got := s.backgroundLoops(); got != 2 {
		t.Fatalf("background loops = %d after cancelling the second ctx, want 2: "+
			"the first tier owns the shared lifetime", got)
	}
	select {
	case <-watchdog.Done:
		t.Fatal("the watchdog stopped even though its own ctx is not the shared one")
	default:
	}

	// Cancelling the owner's context does take everything down.
	cancelOwner()
	waitFor(t, "both loops to stop", 2*time.Second, func() bool { return s.backgroundLoops() == 0 })
	select {
	case <-watchdog.Done:
	case <-time.After(2 * time.Second):
		t.Error("cancelling the first ctx must close the watchdog's Done channel")
	}
}
