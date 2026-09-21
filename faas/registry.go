package faas

import "sync"

// Registry owns function metadata and version history. Version records are
// immutable; rollback only changes the current pointer and preserves history.
type Registry struct {
	mu        sync.RWMutex
	functions map[string]*FunctionRecord
}

func NewRegistry() *Registry {
	return &Registry{functions: make(map[string]*FunctionRecord)}
}

func validateSpec(spec FunctionSpec) error {
	if spec.Name == "" {
		return invalidArgument("function name is empty")
	}
	if (spec.Handler == nil) == (spec.Command == nil) {
		return invalidArgument("function %q must define exactly one of Handler or Command", spec.Name)
	}
	if spec.TimeoutMillis <= 0 {
		return invalidArgument("function %q timeout must be positive", spec.Name)
	}
	if spec.MaxConcurrency <= 0 {
		return invalidArgument("function %q concurrency limit must be positive", spec.Name)
	}
	if spec.Retries.MaxAttempts <= 0 {
		return invalidArgument("function %q retry max attempts must be positive", spec.Name)
	}
	return nil
}

func cloneSpec(spec FunctionSpec) FunctionSpec {
	if spec.Command != nil {
		command := *spec.Command
		if command.Args != nil {
			command.Args = append([]string(nil), command.Args...)
		}
		if command.Env != nil {
			command.Env = append([]string(nil), command.Env...)
		}
		spec.Command = &command
	}
	if spec.Retries.Retryable != nil {
		retryable := make(map[FailureClass]bool, len(spec.Retries.Retryable))
		for class, allowed := range spec.Retries.Retryable {
			retryable[class] = allowed
		}
		spec.Retries.Retryable = retryable
	}
	return spec
}

// Register creates an empty function record. It does not make any version
// callable; a publish is required before invocation.
func (r *Registry) Register(spec FunctionSpec) error {
	if err := validateSpec(spec); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.functions[spec.Name]; exists {
		return conflict("function %q is already registered", spec.Name)
	}
	r.functions[spec.Name] = &FunctionRecord{
		Name: spec.Name,
		Next: 1,
	}
	return nil
}

// RegisterOrPublish is convenient for local programs and tests: it registers a
// function when absent and always publishes one new immutable version.
func (r *Registry) RegisterOrPublish(spec FunctionSpec) (PublishedVersion, error) {
	if err := validateSpec(spec); err != nil {
		return PublishedVersion{}, err
	}
	r.mu.Lock()
	if _, exists := r.functions[spec.Name]; !exists {
		r.functions[spec.Name] = &FunctionRecord{Name: spec.Name, Next: 1}
	}
	rec := r.functions[spec.Name]
	version := PublishedVersion{
		Function: spec.Name,
		Number:   rec.Next,
		State:    VersionAvailable,
		Spec:     cloneSpec(spec),
	}
	rec.Versions = append(rec.Versions, version)
	rec.Current = version.Number
	rec.Next++
	result := version.clone()
	r.mu.Unlock()
	return result, nil
}

// Publish appends a version and atomically points current at it. Old versions
// remain in Versions and can be selected or restored later.
func (r *Registry) Publish(name string, spec FunctionSpec) (PublishedVersion, error) {
	if name == "" {
		return PublishedVersion{}, invalidArgument("function name is empty")
	}
	spec.Name = name
	if err := validateSpec(spec); err != nil {
		return PublishedVersion{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.functions[name]
	if !ok {
		return PublishedVersion{}, notFound("function %q is not registered", name)
	}
	version := PublishedVersion{
		Function: name,
		Number:   rec.Next,
		State:    VersionAvailable,
		Spec:     cloneSpec(spec),
	}
	rec.Versions = append(rec.Versions, version)
	rec.Current = version.Number
	rec.Next++
	return version.clone(), nil
}

// Rollback points current to an existing, available historical version. It
// never resurrects an unavailable version or fabricates a default version.
func (r *Registry) Rollback(name string, target VersionNumber) (PublishedVersion, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.functions[name]
	if !ok {
		return PublishedVersion{}, notFound("function %q is not registered", name)
	}
	if target <= NoVersion {
		return PublishedVersion{}, invalidArgument("function %q version %d is not a positive published version", name, target)
	}
	if int64(target) >= int64(rec.Next) {
		return PublishedVersion{}, notFound("function %q version %d does not exist", name, target)
	}
	for i := range rec.Versions {
		if rec.Versions[i].Number != target {
			continue
		}
		if rec.Versions[i].State != VersionAvailable {
			return PublishedVersion{}, unavailable("function %q version %d is %s", name, target, rec.Versions[i].State)
		}
		rec.Current = target
		return rec.Versions[i].clone(), nil
	}
	return PublishedVersion{}, notFound("function %q version %d is missing from history", name, target)
}

func (r *Registry) Current(name string) (PublishedVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.functions[name]
	if !ok {
		return PublishedVersion{}, notFound("function %q is not registered", name)
	}
	if rec.Current == NoVersion {
		return PublishedVersion{}, unavailable("function %q has no published version", name)
	}
	for _, version := range rec.Versions {
		if version.Number == rec.Current {
			if version.State != VersionAvailable {
				return PublishedVersion{}, unavailable("function %q current version %d is %s", name, version.Number, version.State)
			}
			return version.clone(), nil
		}
	}
	return PublishedVersion{}, platformError("function %q current pointer %d has no version record", name, rec.Current)
}

func (r *Registry) Version(name string, version VersionNumber) (PublishedVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.functions[name]
	if !ok {
		return PublishedVersion{}, notFound("function %q is not registered", name)
	}
	if version == NoVersion {
		return PublishedVersion{}, invalidArgument("function %q version zero is not a published version", name)
	}
	for _, candidate := range rec.Versions {
		if candidate.Number == version {
			if candidate.State != VersionAvailable {
				return PublishedVersion{}, unavailable("function %q version %d is %s", name, version, candidate.State)
			}
			return candidate.clone(), nil
		}
	}
	return PublishedVersion{}, notFound("function %q version %d does not exist", name, version)
}

func (r *Registry) SetAvailable(name string, version VersionNumber, available bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.functions[name]
	if !ok {
		return notFound("function %q is not registered", name)
	}
	for i := range rec.Versions {
		if rec.Versions[i].Number != version {
			continue
		}
		if available {
			rec.Versions[i].State = VersionAvailable
		} else {
			rec.Versions[i].State = VersionUnavailable
		}
		return nil
	}
	return notFound("function %q version %d does not exist", name, version)
}
