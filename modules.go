package modules

import (
	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/load"
)

// LoadInMemoryModule loads the InMemory Module
func (m *Mod) LoadInMemoryModule() {
	load.ModuleInMemory()
}

// LoadModule loads the InMemory Module
func (m *Mod) LoadModule(module coop.Module) {
	load.ModuleAdd(module)
}
