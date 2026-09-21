package faas

import (
	"strconv"
	"sync"
	"time"
)

type InstanceState string

const (
	InstanceCold      InstanceState = "cold"
	InstanceIdle      InstanceState = "idle"
	InstanceBusy      InstanceState = "busy"
	InstanceDraining  InstanceState = "draining"
	InstanceReclaimed InstanceState = "reclaimed"
)

type Instance struct {
	ID       string
	Function string
	Version  VersionNumber
	State    InstanceState
	Uses     int
	LastUsed time.Time
}

type instanceKey struct {
	function string
	version  VersionNumber
}

// InstancePool is an in-process, per-version pool. It also enforces the hard
// in-flight limit: rejected calls never acquire or reserve an instance.
type InstancePool struct {
	mu        sync.Mutex
	nextID    int64
	instances map[string]*Instance
	idle      map[instanceKey]map[string]*Instance
	inflight  map[instanceKey]int
	coldStart int
}

func NewInstancePool() *InstancePool {
	return &InstancePool{
		instances: make(map[string]*Instance),
		idle:      make(map[instanceKey]map[string]*Instance),
		inflight:  make(map[instanceKey]int),
	}
}

// Acquire reuses an idle instance or creates a cold one. A draining/reclaimed
// instance can never be selected. The limit is checked in the same critical
// section as the inflight increment.
func (p *InstancePool) Acquire(function string, version VersionNumber, maxInflight int) (*Instance, error) {
	if function == "" {
		return nil, invalidArgument("instance pool function is empty")
	}
	if version == NoVersion {
		return nil, invalidArgument("instance pool version is zero")
	}
	if maxInflight <= 0 {
		return nil, invalidArgument("instance pool concurrency limit must be positive")
	}
	key := instanceKey{function: function, version: version}
	p.mu.Lock()
	defer p.mu.Unlock()

	current := p.inflight[key]
	if current >= maxInflight {
		return nil, concurrencyLimit("function %s@%d has %d/%d inflight calls", function, version, current, maxInflight)
	}

	ids := p.idle[key]
	for id, inst := range ids {
		delete(ids, id)
		inst.State = InstanceBusy
		inst.LastUsed = time.Now()
		p.inflight[key]++
		copy := *inst
		return &copy, nil
	}
	if len(ids) == 0 {
		delete(p.idle, key)
	}

	p.nextID++
	p.coldStart++
	inst := &Instance{
		ID:       instanceID(function, version, p.nextID),
		Function: function,
		Version:  version,
		State:    InstanceBusy,
		Uses:     1,
		LastUsed: time.Now(),
	}
	p.instances[inst.ID] = inst
	p.inflight[key]++
	result := *inst
	return &result, nil
}

func instanceID(function string, version VersionNumber, id int64) string {
	return function + "-v" + strconv.Itoa(int(version)) + "-i" + strconv.Itoa(int(id))
}

// Release returns a busy instance to the idle set. If it was marked draining
// while the call was running, it is reclaimed exactly once and never idled.
func (p *InstancePool) Release(inst *Instance) error {
	if inst == nil {
		return invalidArgument("cannot release nil instance")
	}
	key := instanceKey{function: inst.Function, version: inst.Version}
	p.mu.Lock()
	defer p.mu.Unlock()

	current, ok := p.instances[inst.ID]
	if !ok {
		return platformError("instance %s is unknown to pool", inst.ID)
	}
	if current.State != InstanceBusy && current.State != InstanceDraining {
		return conflict("instance %s cannot be released from state %s", inst.ID, current.State)
	}
	p.inflight[key]--
	if p.inflight[key] < 0 {
		p.inflight[key] = 0
	}
	if current.State == InstanceDraining {
		current.State = InstanceReclaimed
		current.LastUsed = time.Now()
		return nil
	}
	current.State = InstanceIdle
	current.LastUsed = time.Now()
	if p.idle[key] == nil {
		p.idle[key] = make(map[string]*Instance)
	}
	p.idle[key][current.ID] = current
	return nil
}

// Retire marks a currently busy instance for reclaim. It is used after timeout
// or a handler failure so a possibly polluted in-process instance is never put
// back into the idle set.
func (p *InstancePool) Retire(inst *Instance) {
	if inst == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.instances[inst.ID]
	if !ok {
		return
	}
	if current.State == InstanceBusy {
		current.State = InstanceDraining
	}
}

// ReclaimIdle reclaims idle instances older than maxIdleAge. Busy and draining
// instances are left untouched; zero age reclaims all currently idle instances.
func (p *InstancePool) ReclaimIdle(maxIdleAge time.Duration) int {
	if maxIdleAge < 0 {
		maxIdleAge = 0
	}
	cutoff := time.Now().Add(-maxIdleAge)
	p.mu.Lock()
	defer p.mu.Unlock()
	reclaimed := 0
	for key, ids := range p.idle {
		for id, inst := range ids {
			if inst.State != InstanceIdle || inst.LastUsed.After(cutoff) {
				continue
			}
			inst.State = InstanceReclaimed
			delete(ids, id)
			delete(p.instances, id)
			reclaimed++
		}
		if len(ids) == 0 {
			delete(p.idle, key)
		}
	}
	return reclaimed
}

// DrainVersion prevents a version's idle instances from receiving calls and
// asks its busy instances to be reclaimed when their in-flight call finishes.
func (p *InstancePool) DrainVersion(function string, version VersionNumber) int {
	key := instanceKey{function: function, version: version}
	p.mu.Lock()
	defer p.mu.Unlock()
	affected := 0
	for id, inst := range p.idle[key] {
		inst.State = InstanceReclaimed
		delete(p.instances, id)
		affected++
	}
	delete(p.idle, key)
	for _, inst := range p.instances {
		if inst.Function == function && inst.Version == version && inst.State == InstanceBusy {
			inst.State = InstanceDraining
			affected++
		}
	}
	return affected
}

func (p *InstancePool) Snapshot() []Instance {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Instance, 0, len(p.instances))
	for _, inst := range p.instances {
		out = append(out, *inst)
	}
	return out
}

func (p *InstancePool) ColdStarts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.coldStart
}
