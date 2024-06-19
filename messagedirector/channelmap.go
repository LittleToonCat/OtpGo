package messagedirector

import (
	. "otpgo/util"
	"slices"
	"sort"
	"sync"
)

// TODO: Rewrite everything for efficiency

var lock sync.Mutex
var rangeLock sync.Mutex
var channelMap *ChannelMap

type MDDatagram struct {
	dg       *DatagramIterator
	sender   MDParticipant
	sent     []*Subscriber
	sendLock sync.Mutex
}

func (m *MDDatagram) HasSent(p *Subscriber) bool {
	for _, sub := range m.sent {
		if sub == p {
			return true
		}
	}
	return false
}

type Range struct {
	Min Channel_t
	Max Channel_t
}

func (r Range) Size() Channel_t {
	return r.Max - r.Min
}

type RangeMap struct {
	subscribers  []*Subscriber
	intervals    map[Range][]*Subscriber
	intervalSubs map[*Subscriber][]Range
}

func NewRangeMap() *RangeMap {
	rm := &RangeMap{}
	rm.intervals = make(map[Range][]*Subscriber, 0)
	rm.intervalSubs = make(map[*Subscriber][]Range, 0)
	return rm
}

// Returns an empty range or a list of ranges a subscriber is subscribed to; should usually never return []
func (r *RangeMap) Ranges(p *Subscriber) []Range {
	if rngs, ok := r.intervalSubs[p]; ok {
		return rngs
	} else {
		return make([]Range, 0)
	}
}

// Splits a range; e.g. [================] => [=======][==========]
func (r *RangeMap) Split(rng Range, hi Channel_t, mid Channel_t, lo Channel_t, forward bool) Range {
	irng := r.intervals[rng]
	rnglo := Range{lo, mid}
	rnghi := Range{mid + 1, hi}

	tempSplitSlice := make([]Range, 0)

	for _, sub := range irng {
		intSubs := r.intervalSubs[sub]
		for _, srng := range intSubs {
			if srng != rng && srng != rnglo && srng != rnghi {
				tempSplitSlice = append(tempSplitSlice, srng)
			}
		}
		intSubs = tempSplitSlice
		intSubs = addRange(intSubs, rnghi)
		intSubs = addRange(intSubs, rnglo)
		r.intervalSubs[sub] = intSubs
		sub.ranges = intSubs
	}

	r.intervals[rnglo] = append(make([]*Subscriber, 0, len(irng)), irng...)
	r.intervals[rnghi] = append(make([]*Subscriber, 0, len(irng)), irng...)
	delete(r.intervals, rng)

	if forward {
		return rnghi
	} else {
		return rnglo
	}
}

func addSub(slice []*Subscriber, s *Subscriber) []*Subscriber {
	for _, sub := range slice {
		if sub == s {
			return slice
		}
	}
	return append(slice, s)
}

func addRange(slice []Range, r Range) []Range {
	for _, rng := range slice {
		if rng == r {
			return slice
		}
	}
	return append(slice, r)
}

func (r *RangeMap) Add(rng Range, sub *Subscriber) {
	rangeLock.Lock()
	r.add(rng, sub)
	MD.AddRange(rng.Min, rng.Max)
	rangeLock.Unlock()
}

func (r *RangeMap) add(rng Range, sub *Subscriber) {
	for erng, _ := range r.intervals {
		if rng == erng {
			break
		}

		// {xxxxxx[========]xxxxxxxx}
		if erng.Min > rng.Min && erng.Max < rng.Max {
			r.intervals[erng] = addSub(r.intervals[erng], sub)
			r.intervalSubs[sub] = addRange(r.intervalSubs[sub], erng)
			r.add(Range{rng.Min, erng.Min - 1}, sub)
			r.add(Range{erng.Max + 1, rng.Max}, sub)
			return
		}

		// [======{xxxxxxxx}========]
		if erng.Min < rng.Min && erng.Max > rng.Max {
			rng1 := r.Split(erng, erng.Max, rng.Min-1, erng.Min, true)
			rng2 := r.Split(rng1, rng1.Max, rng.Max, rng1.Min, false)
			r.intervals[rng2] = addSub(r.intervals[rng2], sub)
			r.intervalSubs[sub] = addRange(r.intervalSubs[sub], rng2)
			return
		}

		// [=============={xxxx]xxxx}
		if rng.Min >= erng.Min && rng.Min <= erng.Max && rng.Max > erng.Max {
			if rng.Min == erng.Min {
				r.intervals[erng] = addSub(r.intervals[erng], sub)
				r.intervalSubs[sub] = addRange(r.intervalSubs[sub], erng)
				r.add(Range{erng.Max + 1, rng.Max}, sub)
				return
			}
			nrng := r.Split(erng, erng.Max, rng.Min-1, erng.Min, true)
			r.intervals[nrng] = addSub(r.intervals[nrng], sub)
			r.intervalSubs[sub] = addRange(r.intervalSubs[sub], nrng)
			r.add(Range{erng.Max + 1, rng.Max}, sub)
			return
		}

		// {xxxx[xxxx}==============]
		if rng.Max >= erng.Min && rng.Max <= erng.Max && rng.Min < erng.Min {
			if rng.Max == erng.Max {
				r.intervals[erng] = addSub(r.intervals[erng], sub)
				r.intervalSubs[sub] = addRange(r.intervalSubs[sub], erng)
				r.add(Range{rng.Min, erng.Min - 1}, sub)
				return
			}
			nrng := r.Split(erng, erng.Max, rng.Max, erng.Min, false)
			r.intervals[nrng] = addSub(r.intervals[nrng], sub)
			r.intervalSubs[sub] = addRange(r.intervalSubs[sub], nrng)
			r.add(Range{rng.Min, erng.Min - 1}, sub)
			return
		}
	}

	r.intervals[rng] = addSub(r.intervals[rng], sub)
	r.intervalSubs[sub] = addRange(r.intervalSubs[sub], rng)
	return
}

func (r *RangeMap) removeSubInterval(p *Subscriber, rmv Range) {
	tempSubIntervalSlice := make([]Range, 0)
	if rng, ok := r.intervalSubs[p]; ok {
		for _, v := range rng {
			if v != rmv {
				tempSubIntervalSlice = append(tempSubIntervalSlice, v)
			}
		}
		r.intervalSubs[p] = tempSubIntervalSlice
	}
}

func (r *RangeMap) removeIntervalSub(int Range, p *Subscriber) {
	tempIntervalSubSlice := make([]*Subscriber, 0)
	if rng, ok := r.intervals[int]; ok {
		for _, v := range rng {
			if v != p {
				tempIntervalSubSlice = append(tempIntervalSubSlice, v)
			}
		}
		r.intervals[int] = tempIntervalSubSlice
	}
}

func (r *RangeMap) Remove(rng Range, sub *Subscriber) {
	rangeLock.Lock()
	r.remove(rng, sub, false)
	rangeLock.Unlock()
}

func (r *RangeMap) remove(rng Range, sub *Subscriber, nested bool) {
	for erng, _ := range r.intervals {
		// {xxxxxx[========]xxxxxxxx}
		if erng.Min >= rng.Min && erng.Max <= rng.Max {
			r.removeSubInterval(sub, erng)
			r.removeIntervalSub(erng, sub)
			if erng == rng {
				break
			}
			r.remove(Range{rng.Min, erng.Min - 1}, sub, true)
			r.remove(Range{erng.Max + 1, rng.Max}, sub, true)
			break
		}

		// [======{xxxxxxxx}========]
		if erng.Min < rng.Min && erng.Max > rng.Max {
			rng1 := r.Split(erng, erng.Max, rng.Min-1, erng.Min, true)
			rng2 := r.Split(rng1, rng1.Max, rng.Max, rng1.Min, false)
			r.removeSubInterval(sub, rng2)
			r.removeIntervalSub(rng2, sub)
			break
		}

		// [=============={xxxx]xxxx}
		if rng.Min >= erng.Min && rng.Min <= erng.Max && rng.Max >= erng.Max {
			if rng.Min == erng.Min {
				r.removeSubInterval(sub, erng)
				r.removeIntervalSub(erng, sub)
				r.remove(Range{erng.Max + 1, rng.Max}, sub, true)
				break
			}
			nrng := r.Split(erng, erng.Max, rng.Min-1, erng.Min, true)
			r.removeSubInterval(sub, nrng)
			r.removeIntervalSub(nrng, sub)
			r.remove(Range{nrng.Max + 1, rng.Max}, sub, true)
			break
		}

		// {xxxx[xxxx}==============]
		if rng.Max >= erng.Min && rng.Max <= erng.Max && rng.Min <= erng.Min {
			if rng.Max == erng.Max {
				r.removeSubInterval(sub, erng)
				r.removeIntervalSub(erng, sub)
				r.remove(Range{rng.Min, erng.Min - 1}, sub, true)
				break
			}
			nrng := r.Split(erng, erng.Max, rng.Max, erng.Min, false)
			r.removeSubInterval(sub, nrng)
			r.removeIntervalSub(nrng, sub)
			r.remove(Range{rng.Min, erng.Min - 1}, sub, true)
			break
		}
	}

	// To ensure efficiency upstream, we must send precisely which intervals have gone silent
	if !nested {
		var emptyRanges, finalRanges []Range
		for erng, sublist := range r.intervals {
			if rng.Min <= erng.Min && rng.Max >= erng.Max { // Interval is within the range
				if len(sublist) == 0 {
					emptyRanges = append(emptyRanges, erng)
					delete(r.intervals, erng)
				}
			}
		}
		if len(emptyRanges) == 0 {
			return
		}

		// Sort list of ranges to be joined
		sort.Slice(emptyRanges, func(i, j int) bool {
			return emptyRanges[i].Min < emptyRanges[j].Min
		})
		// Go through the list and join where possible
		if len(emptyRanges) > 1 {
			for n := 0; n < len(emptyRanges); {
				nrng := emptyRanges[n]
				for n != len(emptyRanges)-1 && emptyRanges[n+1].Min == nrng.Max+1 {
					nrng.Max = emptyRanges[n+1].Max
					n++
					if n == len(emptyRanges)-1 {
						break
					}
				}

				finalRanges = append(finalRanges, nrng)
				n++
			}
		} else {
			finalRanges = append(finalRanges, emptyRanges[0])
		}

		// Send out final calculated ranges
		for _, erng := range finalRanges {
			MD.RemoveRange(erng.Min, erng.Max)
		}
	}
}

func (r *RangeMap) Send(ch Channel_t, data *MDDatagram) {
	rangeLock.Lock()
	defer rangeLock.Unlock()

	data.sendLock.Lock()

	for rng, subs := range r.intervals {
		if rng.Min <= ch && rng.Max >= ch {
			for _, sub := range subs {
				if data.sender == nil || sub.participant.Subscriber() != data.sender.Subscriber() {
					// found = true
					if !data.HasSent(sub.participant.Subscriber()) {
						data.sent = append(data.sent, sub.participant.Subscriber())
						sub.participant.HandleDatagram(*data.dg.Dg, data.dg.Copy())
					}
				}
			}
		}
	}

	data.sendLock.Unlock()
}

// Each MD participant is represented as a subscriber within the MD; when a participant desires to listen to
//  a DO (a "channel") the channel map will store it's ID in the participant's unique object.
type Subscriber struct {
	participant MDParticipant

	channels []Channel_t
	ranges   []Range

	active bool
}

type ChannelMap struct {
	subscriptions map[Channel_t][]*Subscriber

	// Ranges points to a RangeMap singularity
	ranges *RangeMap
}

func (s *Subscriber) Subscribed(ch Channel_t) bool {
	for _, c := range s.channels {
		if c == ch {
			return true
		}
	}

	for _, rng := range s.ranges {
		if rng.Min <= ch && rng.Max >= ch {
			return true
		}
	}

	return false
}

func (c *ChannelMap) init() {
	c.subscriptions = map[Channel_t][]*Subscriber{}
	c.ranges = NewRangeMap()
}

func (c *ChannelMap) SubscribeRange(p *Subscriber, rng Range) {
	// Remove single-channel subscriptions; we can't risk data being sent twice
	for _, ch := range p.channels {
		if rng.Min <= ch && rng.Max >= ch {
			c.UnsubscribeChannel(p, ch)
		}
	}

	c.ranges.Add(rng, p)
	p.ranges = c.ranges.Ranges(p)

	MDLog.Debugf("%s has subscribed to range %d - %d", p.participant.Name(), rng.Min, rng.Max)

}

func (c *ChannelMap) UnsubscribeRange(p *Subscriber, rng Range) {
	c.ranges.Remove(rng, p)
	p.ranges = c.ranges.Ranges(p)

	MDLog.Debugf("%s has unsubscribed from range %d - %d", p.participant.Name(), rng.Min, rng.Max)

}

func (c *ChannelMap) UnsubscribeChannel(p *Subscriber, ch Channel_t) {
	if !p.Subscribed(ch) {
		return
	}

	lock.Lock()

	subs := c.subscriptions[ch];
	if len(subs) > 0 {
		i := slices.Index(subs, p)
		// Delete element. We have to recreate the slice, otherwise participants may get stuck in the backing array.
		tempSubsSlice := make([]*Subscriber, 0)
		tempSubsSlice = append(subs[:i], subs[i+1:]...)
		subs = tempSubsSlice
		for n, userCh := range p.channels {
			if userCh == ch {
				tempChannelsSlice := make([]Channel_t, 0)
				tempChannelsSlice = append(p.channels[:n], p.channels[n+1:]...)
				p.channels = tempChannelsSlice
			}
		}
	} else {
		c.ranges.Remove(Range{ch, ch}, p)
	}

	MDLog.Debugf("%s has unsubscribed from channel %d", p.participant.Name(), ch)

	if len(subs) == 0 {
		delete(c.subscriptions, ch)
	} else {
		c.subscriptions[ch] = subs;
	}
	lock.Unlock()

	if !c.IsAnySubscribed(ch) {
		MD.RemoveChannel(ch)
	}
}

func (c *ChannelMap) UnsubscribeAll(p *Subscriber) {
	if len(p.ranges) > 0 {
		oldRanges := make([]Range, len(p.ranges))
		copy(oldRanges, p.ranges)
		for _, rng := range oldRanges {
			c.UnsubscribeRange(p, rng)
		}
	}

	oldChannels := make([]Channel_t, len(p.channels))
	copy(oldChannels, p.channels)
	for _, ch := range oldChannels {
		c.UnsubscribeChannel(p, ch)
	}
}

func (c *ChannelMap) SubscribeChannel(p *Subscriber, ch Channel_t) {
	if p.Subscribed(ch) {
		return
	}

	lock.Lock()
	c.subscriptions[ch] = append(c.subscriptions[ch], p)
	p.channels = append(p.channels, ch)
	lock.Unlock()

	MDLog.Debugf("%s has subscribed to channel %d", p.participant.Name(), ch)

	if len(c.subscriptions[ch]) == 1 {
		MD.AddChannel(ch)
	}
}

func (c *ChannelMap) Send(ch Channel_t, data *MDDatagram) {
	lock.Lock()
	subs := make([]*Subscriber, len(c.subscriptions[ch]))
	copy(subs, c.subscriptions[ch])
	lock.Unlock()
	if len(subs) > 0 {
		data.sendLock.Lock()
		for _, sub := range subs {
			if data.sender == nil || sub.participant.Subscriber() != data.sender.Subscriber() {
				if !data.HasSent(sub.participant.Subscriber()) {
					data.sent = append(data.sent, sub.participant.Subscriber())
					sub.participant.HandleDatagram(*data.dg.Dg, data.dg.Copy())
				}
			}
		}
		data.sendLock.Unlock()
	}
	c.ranges.Send(ch, data)
}

func (c *ChannelMap) IsAnySubscribed(ch Channel_t) bool {
	if len(c.subscriptions[ch]) > 0 {
		return true
	}

	for rng := range c.ranges.intervals {
		if rng.Min <= ch && rng.Max >= ch {
			return true
		}
	}

	return false
}

func init() {
	channelMap = &ChannelMap{}
	channelMap.init()
}
