package coop

const (
	// ModuleCount - Count of the Total number of Package Modules.
	ModuleCount = 1
)

var (
	// PackageModules is the collection of Modules Available in this package.
	PackageModules [ModuleCount]Coordinator
	// OutsideModules is the collection of any outside loaded Modules.
	OutsideModules []Coordinator
)

// Coordinator is a common interface for all subsystem coordinators so that the core routine can manage them in a
// consistent manner. The interface provides a way to configure the coordinator, and then methods to start it and stop
// it safely. It is expected that when any of these funcs are called, the coordinator will then call the corresponding
// func on its modules.
//
// The struct that implements this interface is expected to have an App and Log literal, at the very least. The App
// literal will contain the protocol.ApplicationContext object with resources that the coordinator and modules may use.
// The Log literal will be set up with a logger that has fields set that identify the coordinator. These are set up by
// the core routine before Configure is called. The coordinator can use Log to create the individual loggers for the
// modules it controls.
type Coordinator interface {
	// Configure is called to initially set up the coordinator. In this func, it should validate any configurations
	// that the coordinator requires, and then call the Configure func for each of its modules. If there are any errors
	// in configuration, it is expected that this call will panic. The coordinator may also set up data structures that
	// are critical for the subsystem, such as communication channels. It must NOT make any connections to resources
	// outside of the coordinator itself, including either the storage or evaluator channels in the application context.
	Configure()

	// Start is called to start the operation of the coordinator. In this func, the coordinator should call the Start
	// func for any of its modules, and then start any additional logic the coordinator needs to run. This func must
	// return (any running code must be started as a goroutine). If there is a problem starting up, the coordinator
	// should stop anything it has already started and return a non-nil error.
	Start() error

	// Stop is called to stop operation of the coordinator. In this func, the coordinator should call the Stop func for
	// any of its modules, and stop any goroutines that it has started. While it can return an error if there is a
	// problem, the errors are mostly ignored.
	Stop() error
}
