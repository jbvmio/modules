package load

import (
	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage/inmemory"
)

// ModuleInMemory loads the inmemory Module.
func ModuleInMemory() {
	coop.PackageModules[0] = &inmemory.InMemoryModule{}
}

// ModuleAdd adds an outside Module.
func ModuleAdd(module coop.Module) {
	coop.OutsideModules = append(coop.OutsideModules, module)
}
