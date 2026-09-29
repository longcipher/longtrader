package contract

// Ticker is a snapshot of one instrument's market state.
type Ticker struct {
	// Header carries the stream watermark and latency stages.
	Header *EventHeader
	// Symbol is the instrument.
	Symbol string
	// Timestamp is the venue event time.
	Timestamp *Timestamp
	// Bid is the best bid price.
	Bid Decimal
	// BidVolume is the size resting at the best bid.
	BidVolume Decimal
	// Ask is the best ask price.
	Ask Decimal
	// AskVolume is the size resting at the best ask.
	AskVolume Decimal
	// Last is the last traded price.
	Last Decimal
	// High is the 24h high.
	High Decimal
	// Low is the 24h low.
	Low Decimal
	// Open is the 24h open.
	Open Decimal
	// Close is the previous 24h close.
	Close Decimal
	// BaseVolume is the 24h base-asset volume.
	BaseVolume Decimal
	// QuoteVolume is the 24h quote-asset volume.
	QuoteVolume Decimal
	// Change is the 24h absolute price change.
	Change Decimal
	// Percentage is the 24h relative price change.
	Percentage Decimal
	// VWAP is the 24h volume-weighted average price.
	VWAP Decimal
	// Average is the 24h average price.
	Average Decimal
}

// MarshalTo implements Message.
func (m *Ticker) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.String(2, m.Symbol)
	e.Message(3, m.Timestamp)
	e.Message(4, &m.Bid)
	e.Message(5, &m.BidVolume)
	e.Message(6, &m.Ask)
	e.Message(7, &m.AskVolume)
	e.Message(8, &m.Last)
	e.Message(9, &m.High)
	e.Message(10, &m.Low)
	e.Message(11, &m.Open)
	e.Message(12, &m.Close)
	e.Message(13, &m.BaseVolume)
	e.Message(14, &m.QuoteVolume)
	e.Message(15, &m.Change)
	e.Message(16, &m.Percentage)
	e.Message(17, &m.VWAP)
	e.Message(18, &m.Average)
}

// Unmarshal implements Message.
func (m *Ticker) Unmarshal(data []byte) error {
	*m = Ticker{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		case 3:
			return decodeTime(f, &m.Timestamp)
		case 4:
			return decodeSub(f, &m.Bid)
		case 5:
			return decodeSub(f, &m.BidVolume)
		case 6:
			return decodeSub(f, &m.Ask)
		case 7:
			return decodeSub(f, &m.AskVolume)
		case 8:
			return decodeSub(f, &m.Last)
		case 9:
			return decodeSub(f, &m.High)
		case 10:
			return decodeSub(f, &m.Low)
		case 11:
			return decodeSub(f, &m.Open)
		case 12:
			return decodeSub(f, &m.Close)
		case 13:
			return decodeSub(f, &m.BaseVolume)
		case 14:
			return decodeSub(f, &m.QuoteVolume)
		case 15:
			return decodeSub(f, &m.Change)
		case 16:
			return decodeSub(f, &m.Percentage)
		case 17:
			return decodeSub(f, &m.VWAP)
		case 18:
			return decodeSub(f, &m.Average)
		}
		return nil
	})
}

// PriceLevel is one order book level.
type PriceLevel struct {
	// Price is the level price.
	Price Decimal
	// Amount is the size resting at the level.
	Amount Decimal
}

// MarshalTo implements Message.
func (m *PriceLevel) MarshalTo(e *Encoder) {
	e.Message(1, &m.Price)
	e.Message(2, &m.Amount)
}

// Unmarshal implements Message.
func (m *PriceLevel) Unmarshal(data []byte) error {
	*m = PriceLevel{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			return decodeSub(f, &m.Price)
		case 2:
			return decodeSub(f, &m.Amount)
		}
		return nil
	})
}

// OrderBook is a depth snapshot of one instrument.
type OrderBook struct {
	// Header carries the stream watermark; a sequence gap requires a refetch.
	Header *EventHeader
	// Symbol is the instrument.
	Symbol string
	// Timestamp is the venue event time.
	Timestamp *Timestamp
	// Bids are the buy levels, best first.
	Bids []*PriceLevel
	// Asks are the sell levels, best first.
	Asks []*PriceLevel
}

// MarshalTo implements Message.
func (m *OrderBook) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.String(2, m.Symbol)
	e.Message(3, m.Timestamp)
	for _, l := range m.Bids {
		e.Message(4, l)
	}
	for _, l := range m.Asks {
		e.Message(5, l)
	}
}

// Unmarshal implements Message.
func (m *OrderBook) Unmarshal(data []byte) error {
	*m = OrderBook{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		case 3:
			return decodeTime(f, &m.Timestamp)
		case 4, 5:
			if f.Wire != WireBytes {
				return nil
			}
			l := &PriceLevel{}
			if err := l.Unmarshal(f.Data); err != nil {
				return err
			}
			if f.Number == 4 {
				m.Bids = append(m.Bids, l)
			} else {
				m.Asks = append(m.Asks, l)
			}
		}
		return nil
	})
}

// PublicTrade is one public trade print.
type PublicTrade struct {
	// Header carries the stream watermark.
	Header *EventHeader
	// ID is the venue trade id.
	ID string
	// Symbol is the instrument.
	Symbol string
	// Timestamp is the trade time.
	Timestamp *Timestamp
	// Side is the taker side.
	Side TradeSide
	// Price is the trade price.
	Price Decimal
	// Amount is the trade size.
	Amount Decimal
}

// MarshalTo implements Message.
func (m *PublicTrade) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.String(2, m.ID)
	e.String(3, m.Symbol)
	e.Message(4, m.Timestamp)
	e.Int32(5, int32(m.Side))
	e.Message(6, &m.Price)
	e.Message(7, &m.Amount)
}

// Unmarshal implements Message.
func (m *PublicTrade) Unmarshal(data []byte) error {
	*m = PublicTrade{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			m.ID = f.AsString()
		case 3:
			m.Symbol = f.AsString()
		case 4:
			return decodeTime(f, &m.Timestamp)
		case 5:
			m.Side = TradeSide(f.AsInt32())
		case 6:
			return decodeSub(f, &m.Price)
		case 7:
			return decodeSub(f, &m.Amount)
		}
		return nil
	})
}

// OHLCV is one candle on the event-time stream.
type OHLCV struct {
	// Header carries the stream watermark.
	Header *EventHeader
	// Timestamp is the candle open time.
	Timestamp *Timestamp
	// Open is the opening price.
	Open Decimal
	// High is the highest price in the window.
	High Decimal
	// Low is the lowest price in the window.
	Low Decimal
	// Close is the closing price.
	Close Decimal
	// Volume is the traded base volume in the window.
	Volume Decimal
}

// MarshalTo implements Message.
func (m *OHLCV) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.Message(2, m.Timestamp)
	e.Message(3, &m.Open)
	e.Message(4, &m.High)
	e.Message(5, &m.Low)
	e.Message(6, &m.Close)
	e.Message(7, &m.Volume)
}

// Unmarshal implements Message.
func (m *OHLCV) Unmarshal(data []byte) error {
	*m = OHLCV{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			return decodeTime(f, &m.Timestamp)
		case 3:
			return decodeSub(f, &m.Open)
		case 4:
			return decodeSub(f, &m.High)
		case 5:
			return decodeSub(f, &m.Low)
		case 6:
			return decodeSub(f, &m.Close)
		case 7:
			return decodeSub(f, &m.Volume)
		}
		return nil
	})
}

// StreamSubscription is one symbol on one market stream channel.
type StreamSubscription struct {
	// Channel is the stream kind.
	Channel StreamChannel
	// Symbol is the instrument.
	Symbol string
	// Params carries venue-specific subscription parameters.
	Params map[string]string
}

// MarshalTo implements Message.
func (m *StreamSubscription) MarshalTo(e *Encoder) {
	e.Int32(1, int32(m.Channel))
	e.String(2, m.Symbol)
	encodeMap(e, 3, m.Params)
}

// Unmarshal implements Message.
func (m *StreamSubscription) Unmarshal(data []byte) error {
	*m = StreamSubscription{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Channel = StreamChannel(f.AsInt32())
		case 2:
			m.Symbol = f.AsString()
		case 3:
			return decodeMapEntry(f, 3, &m.Params)
		}
		return nil
	})
}

// StreamMarketDataRequest opens a market-data subscription.
type StreamMarketDataRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Subscriptions are the requested streams.
	Subscriptions []*StreamSubscription
	// ResumeToken is the opaque cursor from a previous stream's last event.
	ResumeToken string
}

// MarshalTo implements Message.
func (m *StreamMarketDataRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	for _, s := range m.Subscriptions {
		e.Message(2, s)
	}
	e.String(3, m.ResumeToken)
}

// Unmarshal implements Message.
func (m *StreamMarketDataRequest) Unmarshal(data []byte) error {
	*m = StreamMarketDataRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			s := &StreamSubscription{}
			if err := s.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Subscriptions = append(m.Subscriptions, s)
		case 3:
			m.ResumeToken = f.AsString()
		}
		return nil
	})
}

// MarketDataEvent is one streamed market data event. Header.Sequence
// increments by one per subscription, so a detected gap requires a snapshot
// refetch for order books.
type MarketDataEvent struct {
	// Header carries the stream watermark.
	Header *EventHeader
	// Ticker is set for a ticker event.
	Ticker *Ticker
	// Orderbook is set for an order book event.
	Orderbook *OrderBook
	// Trade is set for a public trade event.
	Trade *PublicTrade
	// OHLCV is set for a candle event.
	OHLCV *OHLCV
	// ResumeToken is the opaque cursor for reconnect-with-replay.
	ResumeToken string
}

// MarshalTo implements Message.
func (m *MarketDataEvent) MarshalTo(e *Encoder) {
	e.Message(1, m.Header)
	e.Message(2, m.Ticker)
	e.Message(3, m.Orderbook)
	e.Message(4, m.Trade)
	e.Message(5, m.OHLCV)
	e.String(6, m.ResumeToken)
}

// Unmarshal implements Message.
func (m *MarketDataEvent) Unmarshal(data []byte) error {
	*m = MarketDataEvent{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Header = &EventHeader{}
			return m.Header.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Ticker = &Ticker{}
			return m.Ticker.Unmarshal(f.Data)
		case 3:
			if f.Wire != WireBytes {
				return nil
			}
			m.Orderbook = &OrderBook{}
			return m.Orderbook.Unmarshal(f.Data)
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			m.Trade = &PublicTrade{}
			return m.Trade.Unmarshal(f.Data)
		case 5:
			if f.Wire != WireBytes {
				return nil
			}
			m.OHLCV = &OHLCV{}
			return m.OHLCV.Unmarshal(f.Data)
		case 6:
			m.ResumeToken = f.AsString()
		}
		return nil
	})
}

// Candle is one OHLCV bar; TimestampMS is Unix milliseconds, the event-time
// convention for candles.
type Candle struct {
	// TimestampMS is the bar open time in Unix milliseconds.
	TimestampMS int64
	// Open is the opening price.
	Open Decimal
	// High is the highest price in the window.
	High Decimal
	// Low is the lowest price in the window.
	Low Decimal
	// Close is the closing price.
	Close Decimal
	// Volume is the traded base volume in the window.
	Volume Decimal
}

// MarshalTo implements Message.
func (m *Candle) MarshalTo(e *Encoder) {
	e.Int64(1, m.TimestampMS)
	e.Message(2, &m.Open)
	e.Message(3, &m.High)
	e.Message(4, &m.Low)
	e.Message(5, &m.Close)
	e.Message(6, &m.Volume)
}

// Unmarshal implements Message.
func (m *Candle) Unmarshal(data []byte) error {
	*m = Candle{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.TimestampMS = f.AsInt64()
		case 2:
			return decodeSub(f, &m.Open)
		case 3:
			return decodeSub(f, &m.High)
		case 4:
			return decodeSub(f, &m.Low)
		case 5:
			return decodeSub(f, &m.Close)
		case 6:
			return decodeSub(f, &m.Volume)
		}
		return nil
	})
}

// SymbolInfo is instrument metadata for one tradable symbol.
type SymbolInfo struct {
	// Name is the unique symbol identifier, e.g. "BTCUSDT".
	Name string
	// DisplayName is the human-readable name.
	DisplayName string
	// BaseAsset is the base currency.
	BaseAsset string
	// QuoteAsset is the quote currency.
	QuoteAsset string
	// ContractSize is the notional per contract.
	ContractSize Decimal
	// TickSize is the minimum price increment.
	TickSize Decimal
}

// MarshalTo implements Message.
func (m *SymbolInfo) MarshalTo(e *Encoder) {
	e.String(1, m.Name)
	e.String(2, m.DisplayName)
	e.String(3, m.BaseAsset)
	e.String(4, m.QuoteAsset)
	e.Message(5, &m.ContractSize)
	e.Message(6, &m.TickSize)
}

// Unmarshal implements Message.
func (m *SymbolInfo) Unmarshal(data []byte) error {
	*m = SymbolInfo{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Name = f.AsString()
		case 2:
			m.DisplayName = f.AsString()
		case 3:
			m.BaseAsset = f.AsString()
		case 4:
			m.QuoteAsset = f.AsString()
		case 5:
			return decodeSub(f, &m.ContractSize)
		case 6:
			return decodeSub(f, &m.TickSize)
		}
		return nil
	})
}

// FetchTickerRequest fetches one ticker snapshot.
type FetchTickerRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbol is the instrument.
	Symbol string
}

// MarshalTo implements Message.
func (m *FetchTickerRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.Symbol)
}

// Unmarshal implements Message.
func (m *FetchTickerRequest) Unmarshal(data []byte) error {
	*m = FetchTickerRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		}
		return nil
	})
}

// FetchTickerResponse carries one ticker snapshot.
type FetchTickerResponse struct {
	// Ticker is the snapshot.
	Ticker *Ticker
}

// MarshalTo implements Message.
func (m *FetchTickerResponse) MarshalTo(e *Encoder) { e.Message(1, m.Ticker) }

// Unmarshal implements Message.
func (m *FetchTickerResponse) Unmarshal(data []byte) error {
	*m = FetchTickerResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.Ticker = &Ticker{}
			return m.Ticker.Unmarshal(f.Data)
		}
		return nil
	})
}

// FetchOrderBookRequest fetches one order book snapshot.
type FetchOrderBookRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbol is the instrument.
	Symbol string
	// Pagination bounds the depth.
	Pagination *Pagination
}

// MarshalTo implements Message.
func (m *FetchOrderBookRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.Symbol)
	e.Message(3, m.Pagination)
}

// Unmarshal implements Message.
func (m *FetchOrderBookRequest) Unmarshal(data []byte) error {
	*m = FetchOrderBookRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		case 3:
			if f.Wire != WireBytes {
				return nil
			}
			m.Pagination = &Pagination{}
			return m.Pagination.Unmarshal(f.Data)
		}
		return nil
	})
}

// FetchOrderBookResponse carries one order book snapshot.
type FetchOrderBookResponse struct {
	// Orderbook is the snapshot.
	Orderbook *OrderBook
	// Page carries the next cursor and total.
	Page *Page
}

// MarshalTo implements Message.
func (m *FetchOrderBookResponse) MarshalTo(e *Encoder) {
	e.Message(1, m.Orderbook)
	e.Message(2, m.Page)
}

// Unmarshal implements Message.
func (m *FetchOrderBookResponse) Unmarshal(data []byte) error {
	*m = FetchOrderBookResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.Orderbook = &OrderBook{}
			return m.Orderbook.Unmarshal(f.Data)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Page = &Page{}
			return m.Page.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetCandlesRequest fetches historical OHLCV candles.
type GetCandlesRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
	// Symbol is the instrument.
	Symbol string
	// Timeframe is the contract's string form, e.g. "1m" or "M1".
	Timeframe string
	// Pagination bounds the page.
	Pagination *Pagination
}

// MarshalTo implements Message.
func (m *GetCandlesRequest) MarshalTo(e *Encoder) {
	e.Message(1, m.ExchangeID)
	e.String(2, m.Symbol)
	e.String(3, m.Timeframe)
	e.Message(4, m.Pagination)
}

// Unmarshal implements Message.
func (m *GetCandlesRequest) Unmarshal(data []byte) error {
	*m = GetCandlesRequest{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		case 2:
			m.Symbol = f.AsString()
		case 3:
			m.Timeframe = f.AsString()
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			m.Pagination = &Pagination{}
			return m.Pagination.Unmarshal(f.Data)
		}
		return nil
	})
}

// GetCandlesResponse is one page of candles.
type GetCandlesResponse struct {
	// Candles are the bars in this page.
	Candles []*Candle
	// Page carries the next cursor and total.
	Page *Page
}

// MarshalTo implements Message.
func (m *GetCandlesResponse) MarshalTo(e *Encoder) {
	for _, c := range m.Candles {
		e.Message(1, c)
	}
	e.Message(2, m.Page)
}

// Unmarshal implements Message.
func (m *GetCandlesResponse) Unmarshal(data []byte) error {
	*m = GetCandlesResponse{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			if f.Wire != WireBytes {
				return nil
			}
			c := &Candle{}
			if err := c.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Candles = append(m.Candles, c)
		case 2:
			if f.Wire != WireBytes {
				return nil
			}
			m.Page = &Page{}
			return m.Page.Unmarshal(f.Data)
		}
		return nil
	})
}

// ListSymbolsRequest lists tradable instruments.
type ListSymbolsRequest struct {
	// ExchangeID selects the backend instance.
	ExchangeID *ExchangeId
}

// MarshalTo implements Message.
func (m *ListSymbolsRequest) MarshalTo(e *Encoder) { e.Message(1, m.ExchangeID) }

// Unmarshal implements Message.
func (m *ListSymbolsRequest) Unmarshal(data []byte) error {
	*m = ListSymbolsRequest{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			m.ExchangeID = &ExchangeId{}
			return m.ExchangeID.Unmarshal(f.Data)
		}
		return nil
	})
}

// ListSymbolsResponse carries the tradable instruments.
type ListSymbolsResponse struct {
	// Symbols are the instrument descriptions.
	Symbols []*SymbolInfo
}

// MarshalTo implements Message.
func (m *ListSymbolsResponse) MarshalTo(e *Encoder) {
	for _, s := range m.Symbols {
		e.Message(1, s)
	}
}

// Unmarshal implements Message.
func (m *ListSymbolsResponse) Unmarshal(data []byte) error {
	*m = ListSymbolsResponse{}
	return Scan(data, func(f Field) error {
		if f.Number == 1 && f.Wire == WireBytes {
			s := &SymbolInfo{}
			if err := s.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Symbols = append(m.Symbols, s)
		}
		return nil
	})
}
