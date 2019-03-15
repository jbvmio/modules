package load

import (
	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage/inmemory"

	"go.uber.org/zap"
)

// ModuleInMemory loads the inmemory Module.
func ModuleInMemory(app *coop.ApplicationContext) {
	coop.PackageModules[0] = &inmemory.Coordinator{
		App: app,
		Log: app.Logger.With(
			zap.String("type", "coordinator"),
			zap.String("name", "inmemory"),
		),
	}
}

// ModuleAdd adds an outside Module.
// This feature is WiP*
func ModuleAdd(app *coop.ApplicationContext, module coop.Coordinator, name string) {
	coop.OutsideModules = append(coop.OutsideModules, module)
}
