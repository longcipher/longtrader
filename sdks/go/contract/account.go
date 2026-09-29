package contract

// Balance is one currency balance of an account.
type Balance struct {
	// Currency is the asset code, e.g. "USDT".
	Currency string
	// Free is the available amount.
	Free Decimal
	// Used is the amount locked in open orders or positions.
	Used Decimal
	// Total is free plus used.
	Total Decimal
}

// MarshalTo implements Message.
func (m *Balance) MarshalTo(e *Encoder) {
	e.String(1, m.Currency)
	e.Message(2, &m.Free)
	e.Message(3, &m.Used)
	e.Message(4, &m.Total)
}

// Unmarshal implements Message.
func (m *Balance) Unmarshal(data []byte) error {
	*m = Balance{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.Currency = f.AsString()
		case 2:
			return decodeSub(f, &m.Free)
		case 3:
			return decodeSub(f, &m.Used)
		case 4:
			return decodeSub(f, &m.Total)
		}
		return nil
	})
}

// AccountInfo is the venue's account description.
type AccountInfo struct {
	// ID is the venue account id.
	ID string
	// Type is the account type, e.g. "spot" or "futures".
	Type string
	// Code is the display code.
	Code string
	// Balances is one entry per currency.
	Balances []*Balance
}

// MarshalTo implements Message.
func (m *AccountInfo) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.Type)
	e.String(3, m.Code)
	for _, b := range m.Balances {
		e.Message(4, b)
	}
}

// Unmarshal implements Message.
func (m *AccountInfo) Unmarshal(data []byte) error {
	*m = AccountInfo{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.Type = f.AsString()
		case 3:
			m.Code = f.AsString()
		case 4:
			if f.Wire != WireBytes {
				return nil
			}
			b := &Balance{}
			if err := b.Unmarshal(f.Data); err != nil {
				return err
			}
			m.Balances = append(m.Balances, b)
		}
		return nil
	})
}

// LedgerEntry is one wallet ledger movement: deposit, withdrawal or transfer.
type LedgerEntry struct {
	// ID is the venue-assigned entry id.
	ID string
	// Currency is the asset code.
	Currency string
	// Direction is "in" or "out".
	Direction string
	// Type is the movement kind, e.g. "deposit" or "transfer".
	Type string
	// Amount is the moved amount, always positive.
	Amount Decimal
	// Timestamp is the event time.
	Timestamp *Timestamp
	// Status is the movement status, e.g. "completed".
	Status string
}

// MarshalTo implements Message.
func (m *LedgerEntry) MarshalTo(e *Encoder) {
	e.String(1, m.ID)
	e.String(2, m.Currency)
	e.String(3, m.Direction)
	e.String(4, m.Type)
	e.Message(5, &m.Amount)
	e.Message(6, m.Timestamp)
	e.String(7, m.Status)
}

// Unmarshal implements Message.
func (m *LedgerEntry) Unmarshal(data []byte) error {
	*m = LedgerEntry{}
	return Scan(data, func(f Field) error {
		switch f.Number {
		case 1:
			m.ID = f.AsString()
		case 2:
			m.Currency = f.AsString()
		case 3:
			m.Direction = f.AsString()
		case 4:
			m.Type = f.AsString()
		case 5:
			return decodeSub(f, &m.Amount)
		case 6:
			if f.Wire != WireBytes {
				return nil
			}
			m.Timestamp = &Timestamp{}
			return m.Timestamp.Unmarshal(f.Data)
		case 7:
			m.Status = f.AsString()
		}
		return nil
	})
}
