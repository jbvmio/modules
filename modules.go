package modules

import (
	"github.com/jbvmio/modules/load"
)

// LoadInMemoryModule loads the InMemory Module
func (m *Mod) LoadInMemoryModule() {
	load.ModuleInMemory(m.app)
}
