// Today's option chains on disk, so a restart does not have to reload them.
//
// Born of 2026-10-28: ORBtrader was restarted at 09:43, after the open. The
// chains loaded at 05:00 lived only in memory, and IB lists about one expiry
// every 5 s however many requests are in flight, so the 40 chains were back
// only at 09:47 — and every option entry in those three minutes failed as
// option_no_chain. A chain is valid for the trading day it was loaded on
// (chain.readyOn), so the file holds exactly that, and a file from another day
// is not read.
package ibkr

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// chainCacheFile is the cache's on-disk form: the day, the underlyings'
// conIds, and every chain ready on that day.
type chainCacheFile struct {
	Day    string            `json:"day"`
	ConIDs map[string]int64  `json:"con_ids"`
	Chains []chainCacheEntry `json:"chains"`
}

type chainCacheEntry struct {
	Symbol      string               `json:"symbol"`
	OptionDelay int                  `json:"option_delay"`
	Currency    string               `json:"currency"`
	Expiry      string               `json:"expiry"`
	Strikes     map[string][]float64 `json:"strikes"`
}

// restoreChainCache seeds the chains loaded earlier today from
// opts.ChainCachePath. Called once, from NewSession. Chains no selector needs
// are dropped by the loader's first tick, as any other.
func (s *Session) restoreChainCache() {
	path := s.opts.ChainCachePath
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			s.optionLog.Printf("Option chain: WARNING cannot read the chain cache %s: %v", path, err)
		}
		return
	}
	var f chainCacheFile
	if err := json.Unmarshal(data, &f); err != nil {
		s.optionLog.Printf("Option chain: WARNING ignoring an unreadable chain cache %s: %v", path, err)
		return
	}
	today := tradingDay(time.Now())
	if f.Day != today {
		return
	}

	s.optChain.mu.Lock()
	for sym, id := range f.ConIDs {
		if id != 0 {
			s.optChain.conIDs[sym] = id
		}
	}
	n := 0
	for _, e := range f.Chains {
		if e.Expiry < today || len(e.Strikes) == 0 {
			continue
		}
		s.optChain.chains[chainKey{e.Symbol, e.OptionDelay}] = &chain{
			day: today, expiry: e.Expiry, strikes: e.Strikes, currency: e.Currency,
		}
		n++
	}
	s.optChain.mu.Unlock()
	s.optionLog.Printf("Option chain: restored %d chains for %s from %s", n, today, path)
}

// saveChainCache writes every chain ready today to opts.ChainCachePath,
// replacing the file whole. Call without s.optChain.mu held.
func (s *Session) saveChainCache() {
	path := s.opts.ChainCachePath
	if path == "" {
		return
	}
	s.optChain.cacheMu.Lock()
	defer s.optChain.cacheMu.Unlock()

	today := tradingDay(time.Now())
	f := chainCacheFile{Day: today, ConIDs: make(map[string]int64)}
	s.optChain.mu.Lock()
	for key, c := range s.optChain.chains {
		if !c.readyOn(today) {
			continue
		}
		f.Chains = append(f.Chains, chainCacheEntry{
			Symbol: key.symbol, OptionDelay: key.optionDelay, Currency: c.currency,
			Expiry: c.expiry, Strikes: c.strikes,
		})
		if id := s.optChain.conIDs[key.symbol]; id != 0 {
			f.ConIDs[key.symbol] = id
		}
	}
	data, err := json.Marshal(f) // strikes slices are replaced, never mutated, so marshal under the lock
	s.optChain.mu.Unlock()
	if err == nil {
		err = writeFileAtomic(path, data)
	}
	if err != nil {
		s.optionLog.Printf("Option chain: WARNING cannot write the chain cache %s: %v", path, err)
	}
}

// writeFileAtomic writes data beside path and renames it into place, so a
// crash mid-write never leaves a torn file for the next start to read.
func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".chaincache-*.tmp")
	if err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return nil
}
