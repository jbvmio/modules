package modules

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jbvmio/modules/coop"
	"github.com/jbvmio/modules/storage"
	"github.com/jbvmio/modules/storage/inmemory"
)

// Mod controls and manages all modules.
type Mod struct {
	app *coop.ApplicationContext
}

// NewMod returns a new Mod
func NewMod(name string) *Mod {
	return &Mod{
		app: coop.NewApplicationContext(name),
	}
}

func (m *Mod) Start() {
	// This makes sure that we panic and run defers correctly
	defer handleExit()

	// Validate and Configure Modules
	m.app.ConfigureModules()

	// Register signal handlers for exiting
	exitChannel := make(chan os.Signal, 1)
	signal.Notify(exitChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)

	// This triggers handleExit (after other defers), which will then call os.Exit properly
	panic(exitCode{m.app.Start(exitChannel)})
}

// StorageChannel returns the underlying Storage Channel
func (m *Mod) StorageChannel() chan *storage.StorageRequest {
	return m.app.StorageChannel
}

// StorageRequest returns a StorageRequest
func (m *Mod) StorageRequest() *storage.StorageRequest {
	return &storage.StorageRequest{}
}

// SendStorageRequest sends a request to the underlying Storage Channel
func (m *Mod) SendStorageRequest(sr *storage.StorageRequest) *storage.StorageResponse {
	var response storage.StorageResponse
	switch {
	case sr.Reply != nil:
		ok := storage.TimeoutSendStorageRequest(m.StorageChannel(), sr, 2)
		if !ok {
			response.Failure = true
		} else {
			r := <-sr.Reply
			response.Failure = false
			if r != nil {
				response.Object = r.(*inmemory.Data).Object
				response.HasObject = true
			}
		}
	default:
		ok := storage.TimeoutSendStorageRequest(m.StorageChannel(), sr, 2)
		if !ok {
			response.Failure = true
		}
	}
	return &response
}

// exitCode wraps a return value for the application
type exitCode struct{ Code int }

func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(exitCode); ok {
			if exit.Code != 0 {
				fmt.Fprintln(os.Stderr, "Failed at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			} else {
				fmt.Fprintln(os.Stderr, "Stopped at", time.Now().Format("January 2, 2006 at 3:04pm (MST)"))
			}

			os.Exit(exit.Code)
		}
		panic(e) // not an exitCode, bubble up
	}
}
