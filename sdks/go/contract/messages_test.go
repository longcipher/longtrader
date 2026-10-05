package contract

import (
	"strings"
	"testing"
)

func mustDecimal(t *testing.T, text string) Decimal {
	t.Helper()
	d, err := ParseDecimal(text)
	if err != nil {
		t.Fatalf("ParseDecimal(%q): %v", text, err)
	}
	return d
}

func TestCreateOrderRequestCarriesSessionID(t *testing.T) {
	price := "64000.25"
	req := &CreateOrderRequest{
		ExchangeID: &ExchangeId{ID: "okx"},
		Order: &OrderRequest{
			ClientOrderID: "abc123",
			Symbol:        "BTC/USDT",
			Type:          OrderTypeLimit,
			Side:          OrderSideBuy,
			Amount:        mustDecimal(t, "0.001"),
			Price:         mustDecimal(t, price),
			TimeInForce:   TimeInForceGTC,
			PostOnly:      true,
		},
		SessionID: "sess-1",
	}
	wire := Marshal(req)

	// The session id is what the host uses to gate the submission and to scope
	// the kill-switch, so assert against the decoded wire bytes rather than
	// against the struct we just built: a regression that dropped the field
	// from the encoder would silently disable both.
	var decoded CreateOrderRequest
	if err := decoded.Unmarshal(wire); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.SessionID != "sess-1" {
		t.Fatalf("CreateOrderRequest.session_id = %q on the wire, want %q; the host would treat this as an unscoped operator call", decoded.SessionID, "sess-1")
	}
	var sessionField *Field
	if err := Scan(wire, func(f Field) error {
		if f.Number == 3 {
			sessionField = &f
		}
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if sessionField == nil {
		t.Fatal("field 3 (session_id) is absent from the encoding")
	}
	if sessionField.Wire != WireBytes {
		t.Errorf("session_id wire type = %d, want %d", sessionField.Wire, WireBytes)
	}
	if sessionField.AsString() != "sess-1" {
		t.Errorf("session_id = %q, want %q", sessionField.AsString(), "sess-1")
	}

	// The rest of the order must survive the trip too.
	if decoded.Order == nil {
		t.Fatal("order is missing")
	}
	if decoded.Order.Symbol != "BTC/USDT" || decoded.Order.Type != OrderTypeLimit || decoded.Order.Side != OrderSideBuy {
		t.Errorf("order header decoded as %+v", decoded.Order)
	}
	if !decoded.Order.PostOnly || decoded.Order.TimeInForce != TimeInForceGTC {
		t.Errorf("order flags decoded as %+v", decoded.Order)
	}
	amount, err := decoded.Order.Amount.Float64()
	if err != nil || amount != 0.001 {
		t.Errorf("amount = %v (%v), want 0.001", amount, err)
	}
	if decoded.Order.Price.Value != price {
		t.Errorf("price = %q, want %q", decoded.Order.Price.Value, price)
	}
}

func TestCreateOrdersRequestCarriesSessionID(t *testing.T) {
	req := &CreateOrdersRequest{
		Orders: []*OrderRequest{
			{Symbol: "BTC/USDT", Side: OrderSideBuy, Type: OrderTypeLimit, Amount: mustDecimal(t, "0.001")},
			{Symbol: "BTC/USDT", Side: OrderSideSell, Type: OrderTypeLimit, Amount: mustDecimal(t, "0.001")},
		},
		SessionID: "sess-2",
	}
	var decoded CreateOrdersRequest
	if err := decoded.Unmarshal(Marshal(req)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.SessionID != "sess-2" {
		t.Errorf("session_id = %q, want sess-2", decoded.SessionID)
	}
	if len(decoded.Orders) != 2 {
		t.Fatalf("got %d orders, want 2", len(decoded.Orders))
	}
	if decoded.Orders[0].Side != OrderSideBuy || decoded.Orders[1].Side != OrderSideSell {
		t.Errorf("sides decoded as %v/%v", decoded.Orders[0].Side, decoded.Orders[1].Side)
	}
}

func TestEmptyRequestEncodesToNothing(t *testing.T) {
	// proto3 omission: an all-default request must be a zero-length body, not
	// a body of zero-valued fields the host would then have to distinguish.
	if got := Marshal(&CreateOrderRequest{}); len(got) != 0 {
		t.Errorf("empty CreateOrderRequest encoded to % x, want no bytes", got)
	}
	if got := Marshal(&KeepAliveRequest{}); len(got) != 0 {
		t.Errorf("empty KeepAliveRequest encoded to % x, want no bytes", got)
	}
}

func TestAttachSessionRequestCarriesToken(t *testing.T) {
	req := &AttachSessionRequest{
		Token:         "s3cr3t",
		ClientName:    "longtrader-sdk-go",
		ClientVersion: "0.2.0",
		Policy: &KillSwitchPolicy{
			LeaseTimeout: &Duration{Seconds: 30},
			Scope:        KillSwitchScopeSessionOrders,
		},
		SessionID: "resume-me",
	}
	var decoded AttachSessionRequest
	if err := decoded.Unmarshal(Marshal(req)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Token != "s3cr3t" {
		t.Errorf("token = %q, want s3cr3t", decoded.Token)
	}
	if decoded.SessionID != "resume-me" {
		t.Errorf("session_id = %q, want resume-me", decoded.SessionID)
	}
	if decoded.Policy == nil || decoded.Policy.Scope != KillSwitchScopeSessionOrders {
		t.Fatalf("policy decoded as %+v", decoded.Policy)
	}
	if got := decoded.Policy.LeaseTimeout.AsDuration().Seconds(); got != 30 {
		t.Errorf("lease timeout = %vs, want 30s", got)
	}
}

func TestAttachSessionResponseRoundTrip(t *testing.T) {
	resp := &AttachSessionResponse{
		SessionID:           "sess-3",
		HeartbeatIntervalMS: 5000,
		ServerTime:          &Timestamp{Seconds: 1_700_000_000, Nanos: 500},
		Capabilities:        []string{"reconcile", "kill_switch", "event_replay"},
	}
	var decoded AttachSessionResponse
	if err := decoded.Unmarshal(Marshal(resp)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.SessionID != "sess-3" || decoded.HeartbeatIntervalMS != 5000 {
		t.Errorf("decoded %+v", decoded)
	}
	if strings.Join(decoded.Capabilities, ",") != "reconcile,kill_switch,event_replay" {
		t.Errorf("capabilities = %q", decoded.Capabilities)
	}
	if decoded.ServerTime == nil || decoded.ServerTime.Nanos != 500 {
		t.Errorf("server time decoded as %+v", decoded.ServerTime)
	}
}

func TestReconcileStateResponseRoundTrip(t *testing.T) {
	resp := &ReconcileStateResponse{
		SnapshotSequence: 4242,
		SnapshotTime:     &Timestamp{Seconds: 1_700_000_000},
		Balances: []*Balance{
			{Currency: "USDT", Free: mustDecimal(t, "1000.5"), Used: mustDecimal(t, "0"), Total: mustDecimal(t, "1000.5")},
		},
		Positions: []*Position{
			{ID: "pos-1", Symbol: "BTC/USDT", Side: OrderSideBuy, Contracts: mustDecimal(t, "2"), EntryPrice: mustDecimal(t, "60000")},
		},
		OpenOrders: []*Order{
			{ID: "ord-1", Symbol: "BTC/USDT", Status: OrderStatusOpen, Amount: mustDecimal(t, "0.5"), Price: mustDecimal(t, "61000")},
		},
	}
	var decoded ReconcileStateResponse
	if err := decoded.Unmarshal(Marshal(resp)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.SnapshotSequence != 4242 {
		t.Errorf("snapshot sequence = %d, want 4242", decoded.SnapshotSequence)
	}
	if len(decoded.Balances) != 1 || decoded.Balances[0].Currency != "USDT" {
		t.Fatalf("balances decoded as %+v", decoded.Balances)
	}
	if got := decoded.Balances[0].Free.MustFloat64(); got != 1000.5 {
		t.Errorf("free balance = %v, want 1000.5", got)
	}
	if len(decoded.Positions) != 1 || decoded.Positions[0].ID != "pos-1" {
		t.Errorf("positions decoded as %+v", decoded.Positions)
	}
	if len(decoded.OpenOrders) != 1 || decoded.OpenOrders[0].Status != OrderStatusOpen {
		t.Errorf("open orders decoded as %+v", decoded.OpenOrders)
	}
}

// A Decimal field always encodes as a (possibly empty) submessage, so an
// absent decimal arrives as the zero value rather than as "no field".
func TestZeroDecimalStillEncodesAsAMessage(t *testing.T) {
	bal := &Balance{Currency: "USDT", Free: mustDecimal(t, "0")}
	var decoded Balance
	if err := decoded.Unmarshal(Marshal(bal)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !decoded.Free.IsZero() {
		t.Errorf("free balance = %+v, want zero", decoded.Free)
	}
	if got := decoded.Free.MustFloat64(); got != 0 {
		t.Errorf("free balance = %v, want 0", got)
	}
}

func TestOrderRoundTripWithMapAndTimestamps(t *testing.T) {
	order := &Order{
		ID:            "ord-2",
		ClientOrderID: "cid",
		Symbol:        "ETH/USDT",
		Type:          OrderTypeStopLimit,
		Side:          OrderSideSell,
		Status:        OrderStatusCanceled,
		Amount:        mustDecimal(t, "1.5"),
		Price:         mustDecimal(t, "3000.125"),
		Filled:        mustDecimal(t, "0.25"),
		Remaining:     mustDecimal(t, "1.25"),
		Cost:          mustDecimal(t, "750.03125"),
		Fee:           mustDecimal(t, "0.75"),
		FeeCurrency:   "USDT",
		TimeInForce:   TimeInForceIOC,
		Timestamp:     &Timestamp{Seconds: 10, Nanos: 20},
		PostOnly:      true,
		ReduceOnly:    true,
		Info:          map[string]string{"source": "grid", "ttl": "30"},
		CreatedAt:     &Timestamp{Seconds: 11},
		UpdatedAt:     &Timestamp{Seconds: 12},
		TakeProfit:    &Decimal{Value: "3100"},
		StopLoss:      &Decimal{Value: "2900"},
	}
	var decoded Order
	if err := decoded.Unmarshal(Marshal(order)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.ID != "ord-2" || decoded.Status != OrderStatusCanceled || decoded.Type != OrderTypeStopLimit {
		t.Errorf("decoded %+v", decoded)
	}
	if decoded.Info["source"] != "grid" || decoded.Info["ttl"] != "30" {
		t.Errorf("info = %v", decoded.Info)
	}
	if decoded.TakeProfit == nil || decoded.TakeProfit.Value != "3100" {
		t.Errorf("take profit = %+v", decoded.TakeProfit)
	}
	if decoded.StopLoss == nil || decoded.StopLoss.Value != "2900" {
		t.Errorf("stop loss = %+v", decoded.StopLoss)
	}
	if got := decoded.Cost.MustFloat64(); got != 750.03125 {
		t.Errorf("cost = %v, want 750.03125", got)
	}
	if decoded.Timestamp == nil || decoded.Timestamp.Nanos != 20 {
		t.Errorf("timestamp = %+v", decoded.Timestamp)
	}
}

func TestModifyPositionOptionalBrackets(t *testing.T) {
	// Absent brackets stay absent, so the host leaves them unchanged.
	var decoded ModifyPositionRequest
	if err := decoded.Unmarshal(Marshal(&ModifyPositionRequest{PositionID: "pos-1"})); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.TakeProfit != nil || decoded.StopLoss != nil {
		t.Errorf("absent brackets decoded as %+v / %+v", decoded.TakeProfit, decoded.StopLoss)
	}

	tp := mustDecimal(t, "3100.5")
	sl := mustDecimal(t, "2900.25")
	wire := Marshal(&ModifyPositionRequest{PositionID: "pos-1", TakeProfit: &tp, StopLoss: &sl})
	var both ModifyPositionRequest
	if err := both.Unmarshal(wire); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if both.TakeProfit == nil || both.TakeProfit.String() != "3100.5" {
		t.Errorf("take profit = %+v", both.TakeProfit)
	}
	if both.StopLoss == nil || both.StopLoss.String() != "2900.25" {
		t.Errorf("stop loss = %+v", both.StopLoss)
	}

	// A stop-only edit must not clear the target.
	stopOnly := mustDecimal(t, "1")
	var partial ModifyPositionRequest
	if err := partial.Unmarshal(Marshal(&ModifyPositionRequest{PositionID: "pos-1", StopLoss: &stopOnly})); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if partial.TakeProfit != nil {
		t.Errorf("a stop-only edit must leave take_profit absent, got %+v", partial.TakeProfit)
	}
}

func TestStrategyEventRoundTrip(t *testing.T) {
	ev := &StrategyEvent{
		Header: &EventHeader{TraceID: "trace-1", Sequence: 7, ExchangeTimeNS: 1, GatewayInTimeNS: 2, LocalDispatchTimeNS: 3},
		OrderUpdate: &OrderUpdate{
			Order:      &Order{ID: "ord-1", Status: OrderStatusFilled},
			UpdateType: "fill",
		},
		ResumeToken: "cursor-1",
	}
	var decoded StrategyEvent
	if err := decoded.Unmarshal(Marshal(ev)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Header == nil || decoded.Header.Sequence != 7 || decoded.Header.TraceID != "trace-1" {
		t.Fatalf("header decoded as %+v", decoded.Header)
	}
	if decoded.OrderUpdate == nil || decoded.OrderUpdate.UpdateType != "fill" {
		t.Fatalf("order update decoded as %+v", decoded.OrderUpdate)
	}
	if decoded.OrderUpdate.Order == nil || decoded.OrderUpdate.Order.Status != OrderStatusFilled {
		t.Errorf("nested order decoded as %+v", decoded.OrderUpdate.Order)
	}
	if decoded.ResumeToken != "cursor-1" {
		t.Errorf("resume token = %q", decoded.ResumeToken)
	}
	// Only the populated oneof arm survives.
	if decoded.Ticker != nil || decoded.Log != nil {
		t.Errorf("unset oneof arms must stay nil: %+v", decoded)
	}
}

func TestLogEventRoundTripWithFields(t *testing.T) {
	ev := &LogEvent{
		SessionID: "sess-1",
		Level:     LogLevelWarn,
		Message:   "grid leg 3 rejected",
		Timestamp: &Timestamp{Seconds: 1_700_000_000, Nanos: 7},
		Fields:    map[string]string{"symbol": "BTC/USDT", "leg": "3"},
	}
	var decoded LogEvent
	if err := decoded.Unmarshal(Marshal(ev)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Level != LogLevelWarn || decoded.Message != "grid leg 3 rejected" {
		t.Errorf("decoded %+v", decoded)
	}
	if decoded.Fields["symbol"] != "BTC/USDT" || decoded.Fields["leg"] != "3" {
		t.Errorf("fields = %v", decoded.Fields)
	}
}

func TestTickerRoundTripWithVenueShape(t *testing.T) {
	// A ticker is four decimals and a header, and every price is one base-10
	// string: there is no second field for a reader to consult or prefer, so
	// the round trip has to carry the payload itself.
	ticker := &Ticker{
		Symbol:    "BTC/USDT",
		Header:    &EventHeader{Sequence: 3},
		Bid:       mustDecimal(t, "63999.9925"),
		Ask:       mustDecimal(t, "64000.0075"),
		Last:      mustDecimal(t, "64000"),
		BidVolume: mustDecimal(t, "15"),
	}
	var decoded Ticker
	if err := decoded.Unmarshal(Marshal(ticker)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Symbol != "BTC/USDT" {
		t.Errorf("symbol = %q", decoded.Symbol)
	}
	if decoded.Header == nil || decoded.Header.Sequence != 3 {
		t.Errorf("header = %+v", decoded.Header)
	}
	if err := decoded.Bid.Validate(); err != nil {
		t.Errorf("bid did not survive the trip: %v", err)
	}
	bid, err := decoded.Bid.Float64()
	if err != nil || bid != 63999.9925 {
		t.Errorf("bid = %v (%v), want 63999.9925", bid, err)
	}
	ask, err := decoded.Ask.Float64()
	if err != nil || ask != 64000.0075 {
		t.Errorf("ask = %v (%v), want 64000.0075", ask, err)
	}
	if decoded.BidVolume.Value != "15" {
		t.Errorf("bid volume = %q, want %q", decoded.BidVolume.Value, "15")
	}
}

func TestOrderBookKeepsBidAskSeparate(t *testing.T) {
	book := &OrderBook{
		Symbol: "BTC/USDT",
		Bids: []*PriceLevel{
			{Price: mustDecimal(t, "100"), Amount: mustDecimal(t, "1")},
			{Price: mustDecimal(t, "99"), Amount: mustDecimal(t, "2")},
		},
		Asks: []*PriceLevel{
			{Price: mustDecimal(t, "101"), Amount: mustDecimal(t, "3")},
		},
	}
	var decoded OrderBook
	if err := decoded.Unmarshal(Marshal(book)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decoded.Bids) != 2 || len(decoded.Asks) != 1 {
		t.Fatalf("levels decoded as %d bids / %d asks", len(decoded.Bids), len(decoded.Asks))
	}
	if got := decoded.Bids[1].Price.MustFloat64(); got != 99 {
		t.Errorf("second bid = %v, want 99", got)
	}
	if got := decoded.Asks[0].Amount.MustFloat64(); got != 3 {
		t.Errorf("ask amount = %v, want 3", got)
	}
}

func TestGetCandlesAndListSymbolsRoundTrip(t *testing.T) {
	candles := &GetCandlesResponse{
		Candles: []*Candle{
			{TimestampMS: 1_700_000_000_000, Open: mustDecimal(t, "1"), High: mustDecimal(t, "2"), Low: mustDecimal(t, "0.5"), Close: mustDecimal(t, "1.5"), Volume: mustDecimal(t, "10")},
		},
		Page: &Page{NextCursor: "c1", Total: 1},
	}
	var decodedCandles GetCandlesResponse
	if err := decodedCandles.Unmarshal(Marshal(candles)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decodedCandles.Candles) != 1 || decodedCandles.Candles[0].TimestampMS != 1_700_000_000_000 {
		t.Errorf("candles decoded as %+v", decodedCandles.Candles)
	}
	if decodedCandles.Page == nil || decodedCandles.Page.NextCursor != "c1" {
		t.Errorf("page decoded as %+v", decodedCandles.Page)
	}

	symbols := &ListSymbolsResponse{Symbols: []*SymbolInfo{
		{Name: "BTCUSDT", DisplayName: "BTC/USDT", BaseAsset: "BTC", QuoteAsset: "USDT", TickSize: mustDecimal(t, "0.1")},
	}}
	var decodedSymbols ListSymbolsResponse
	if err := decodedSymbols.Unmarshal(Marshal(symbols)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decodedSymbols.Symbols) != 1 || decodedSymbols.Symbols[0].Name != "BTCUSDT" {
		t.Errorf("symbols decoded as %+v", decodedSymbols.Symbols)
	}
	if got := decodedSymbols.Symbols[0].TickSize.MustFloat64(); got != 0.1 {
		t.Errorf("tick size = %v, want 0.1", got)
	}
}

func TestClosedPositionsAndTradesRoundTrip(t *testing.T) {
	closed := &GetClosedPositionsResponse{Positions: []*ClosedPosition{
		{
			ID: "cp-1", Symbol: "BTC/USDT", Side: PositionSideShort,
			Quantity: mustDecimal(t, "1"), EntryPrice: mustDecimal(t, "60000"),
			ExitPrice: mustDecimal(t, "59000"), RealizedPnL: mustDecimal(t, "1000"),
			CloseReason: CloseReasonStopLoss,
		},
	}}
	var decodedClosed GetClosedPositionsResponse
	if err := decodedClosed.Unmarshal(Marshal(closed)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(decodedClosed.Positions) != 1 {
		t.Fatalf("closed positions = %+v", decodedClosed.Positions)
	}
	if decodedClosed.Positions[0].Side != PositionSideShort || decodedClosed.Positions[0].CloseReason != CloseReasonStopLoss {
		t.Errorf("closed position decoded as %+v", decodedClosed.Positions[0])
	}

	trade := &Trade{ID: "t-1", OrderID: "o-1", Symbol: "BTC/USDT", Side: OrderSideSell, Price: mustDecimal(t, "59000"), Amount: mustDecimal(t, "0.1"), Maker: true}
	var decodedTrade Trade
	if err := decodedTrade.Unmarshal(Marshal(trade)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decodedTrade.Side != OrderSideSell || !decodedTrade.Maker {
		t.Errorf("trade decoded as %+v", decodedTrade)
	}
}

func TestPaginationRequestRoundTrip(t *testing.T) {
	req := &FetchOpenOrdersRequest{
		ExchangeID: &ExchangeId{ID: "okx", Label: "OKX"},
		Symbol:     "BTC/USDT",
		Pagination: &Pagination{Limit: 50, Since: 1_700_000_000_000, Cursor: "cur"},
	}
	var decoded FetchOpenOrdersRequest
	if err := decoded.Unmarshal(Marshal(req)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Pagination == nil || decoded.Pagination.Limit != 50 || decoded.Pagination.Cursor != "cur" {
		t.Errorf("pagination decoded as %+v", decoded.Pagination)
	}
	if decoded.ExchangeID == nil || decoded.ExchangeID.Label != "OKX" {
		t.Errorf("exchange id decoded as %+v", decoded.ExchangeID)
	}
}

func TestMarketDataEventRoundTrip(t *testing.T) {
	ev := &MarketDataEvent{
		Header:      &EventHeader{Sequence: 9},
		Ticker:      &Ticker{Symbol: "BTC/USDT", Last: mustDecimal(t, "64000")},
		ResumeToken: "rt-1",
	}
	var decoded MarketDataEvent
	if err := decoded.Unmarshal(Marshal(ev)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Header == nil || decoded.Header.Sequence != 9 {
		t.Errorf("header = %+v", decoded.Header)
	}
	if decoded.Ticker == nil || decoded.Ticker.Symbol != "BTC/USDT" {
		t.Errorf("ticker = %+v", decoded.Ticker)
	}
	if decoded.ResumeToken != "rt-1" {
		t.Errorf("resume token = %q", decoded.ResumeToken)
	}
}

func TestStrategyStatusAndStopStrategyRoundTrip(t *testing.T) {
	status := &StrategyStatusResponse{
		State:           SessionStateActive,
		StrategyID:      "strategy-1",
		Name:            "go_grid",
		StartedAt:       &Timestamp{Seconds: 1_700_000_000},
		OrdersSubmitted: 12,
		LogEvents:       34,
	}
	var decodedStatus StrategyStatusResponse
	if err := decodedStatus.Unmarshal(Marshal(status)); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Field 1 was reserved and must never be reused: the state lives at 2.
	if decodedStatus.State != SessionStateActive || decodedStatus.OrdersSubmitted != 12 || decodedStatus.LogEvents != 34 {
		t.Errorf("status decoded as %+v", decodedStatus)
	}

	var decodedStop StopStrategyResponse
	if err := decodedStop.Unmarshal(Marshal(&StopStrategyResponse{FinalState: SessionStateGracefulShutdown})); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decodedStop.FinalState != SessionStateGracefulShutdown {
		t.Errorf("final state = %v", decodedStop.FinalState)
	}
}

func TestEnumParsing(t *testing.T) {
	cases := []struct {
		parse func(string) (int32, error)
		in    string
		want  int32
	}{
		{func(s string) (int32, error) { v, err := ParseOrderType(s); return int32(v), err }, "LIMIT", 2},
		{func(s string) (int32, error) { v, err := ParseOrderType(s); return int32(v), err }, "limit", 2},
		{func(s string) (int32, error) { v, err := ParseOrderType(s); return int32(v), err }, "ORDER_TYPE_STOP_LIMIT", 4},
		{func(s string) (int32, error) { v, err := ParseOrderType(s); return int32(v), err }, "stop-limit", 4},
		{func(s string) (int32, error) { v, err := ParseOrderSide(s); return int32(v), err }, "SELL", 2},
		{func(s string) (int32, error) { v, err := ParseTimeInForce(s); return int32(v), err }, "FOK", 3},
		{func(s string) (int32, error) { v, err := ParseLogLevel(s); return int32(v), err }, "warn", 3},
		{func(s string) (int32, error) { v, err := ParseSessionState(s); return int32(v), err }, "SESSION_STATE_ACTIVE", 3},
		{func(s string) (int32, error) { v, err := ParseStreamChannel(s); return int32(v), err }, "orderbook", 2},
		{func(s string) (int32, error) { v, err := ParseKillSwitchScope(s); return int32(v), err }, "SCOPE_SESSION_ORDERS", 1},
	}
	for _, tc := range cases {
		got, err := tc.parse(tc.in)
		if err != nil {
			t.Errorf("Parse(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Parse(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestEnumParseErrorNamesOptions(t *testing.T) {
	_, err := ParseOrderType("BOGUS")
	if err == nil {
		t.Fatal("expected an error for a typo")
	}
	for _, want := range []string{"MARKET", "LIMIT", "STOP", "STOP_LIMIT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should list %q", err, want)
		}
	}
}

func TestEnumStrings(t *testing.T) {
	if OrderTypeStopLimit.String() != "STOP_LIMIT" {
		t.Errorf("OrderType = %q", OrderTypeStopLimit.String())
	}
	if SessionStateKillSwitchTripped.String() != "KILL_SWITCH_TRIPPED" {
		t.Errorf("SessionState = %q", SessionStateKillSwitchTripped.String())
	}
	if SessionStateUnspecified.String() != "UNSPECIFIED" {
		t.Errorf("SessionState = %q", SessionStateUnspecified.String())
	}
	if got := SessionState(99).String(); !strings.Contains(got, "99") {
		t.Errorf("out-of-range enum = %q, want the numeric value", got)
	}
}

func TestTimestampAndDurationHelpers(t *testing.T) {
	ts := &Timestamp{Seconds: 1_700_000_000, Nanos: 250_000_000}
	if got := ts.AsTime().UnixNano(); got != 1_700_000_000_250_000_000 {
		t.Errorf("AsTime = %d", got)
	}
	d := DurationFrom(90 * 1e9)
	if d.Seconds != 90 || d.Nanos != 0 {
		t.Errorf("DurationFrom = %+v", d)
	}
	if got := (&Duration{Seconds: 2, Nanos: 500_000_000}).AsDuration(); got != 2.5e9 {
		t.Errorf("AsDuration = %v", got)
	}
	if NowTimestamp().Seconds <= 0 {
		t.Error("NowTimestamp should carry a wall-clock second")
	}
}
