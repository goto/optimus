// The standard Go export_test pattern: this file is part of package service, not service_test,
// because the hook below has to reach a private field. Nothing here ships in the binary.
//
//nolint:testpackage // must be in-package to set the unexported resolved field
package service

import "sync"

// WaitGroupForTest makes the detached resolver joinable so tests can assert on its outcome
// without sleeping. Only compiled into the test binary; production code never sets this, and
// the resolver skips the bookkeeping when it is nil.
func (s *RunAttributionService) WaitGroupForTest() *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	s.resolved = wg
	return wg
}
