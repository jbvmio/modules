package coop

import (
	"sync"

	"github.com/jbvmio/modules/storage"
	"go.uber.org/zap"
)

const (
	// ModuleCount - Count of the Total number of Package Modules.
	ModuleCount = 1
)

var (
	// PackageModules is the collection of Modules Available in this package.
	PackageModules [ModuleCount]Module
	// OutsideModules is the collection of any outside loaded Modules.
	OutsideModules []Module
)

// exampleBase is the base construct of a Module.
type exampleBase struct {
	name        string
	class       string
	Log         *zap.Logger
	quitChannel chan struct{}
	running     *sync.WaitGroup
}

// Module is a common interface for all subsystem coordinators so that the core routine can manage them in a
// consistent manner. The interface provides a way to configure the coordinator, and then methods to start it and stop
// it safely. It is expected that when any of these funcs are called, the coordinator will then call the corresponding
// func on its modules.
//
// The struct that implements this interface is expected to have an App and Log literal, at the very least. The App
// literal will contain the protocol.ApplicationContext object with resources that the coordinator and modules may use.
// The Log literal will be set up with a logger that has fields set that identify the coordinator. These are set up by
// the core routine before Configure is called. The coordinator can use Log to create the individual loggers for the
// modules it controls.
type Module interface {
	// Init is responsible for initializing the module with a stop channel and controlling waitgroup.
	// It must accept a chan struct{} which should be used in the modules' Stop() function to stop the module.
	// It must also accept a sync.WaitGroup which should invoke the waitgroup Done() in the modules' Stop() funtion.
	Init(chan struct{}, *sync.WaitGroup)

	// ModuleDetails returns the Module's name and class - ie. storage, http, etc.
	ModuleDetails() (moduleName string, className string)

	// AssignApplicationContext assigns the given *ApplicationContext to the underlying *ApplicationContext.
	AssignApplicationContext(*ApplicationContext)

	// AssignModuleLogger assigns the given *ModuleLogger to the underlying Module Logger.
	AssignModuleLogger(*zap.Logger)

	// AssignModuleLogger returns the underlying Logger for any additional configuration.
	// The main logger uses the zap package so all loadable Modules should use the same for consistency.
	ModuleLogger() *zap.Logger

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

// StorageModule is responsible storing desired data.
// watches. It must accept and respond to all Request types. This interface conforms to the overall
// Module interface, but it adds a func to fetch the channel that the module is listening on for requests, so
// that requests can be forwarded to it by the coordinator.
type StorageModule interface {
	Module
	GetCommunicationChannel() chan *storage.Request
}
