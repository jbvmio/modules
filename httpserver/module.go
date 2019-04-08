package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"
)

var (
	// LogLevel for logging.
	LogLevel string
)

// Configs contain one or more httpserver Config.
type Configs struct {
	HostSwitch string
	Server     map[string]*Config
}

// Module runs the HTTP interface for Burrow, managing all configured listeners.
type Module struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	//App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Logger      *zap.Logger
	Servers     map[string]*HTTPServer
	Switch      HostSwitch
	SwitchPorts []string
	MaxTimeout  int
	Configs     *Configs

	useHS bool
	hsMap map[string]bool
	/*
		Router    *httprouter.Router
		Servers   map[string]*http.Server
		TheCert   map[string]string
		TheKey    map[string]string
		CORSAllow map[string]string
	*/
}

// NewModule returns a new Module with defaults.
func NewModule(configs *Configs) *Module {
	var useHS bool
	var switchPorts []string
	if configs == nil {
		configs = &Configs{
			Server: make(map[string]*Config),
		}
		configs.Server["default"] = NewConfig()
	}
	servers := make(map[string]*HTTPServer, len(configs.Server))
	hostPorts := make(map[string][]string, len(configs.Server))
	hsMap := make(map[string]bool)
	dupe := make(map[string]bool, len(configs.Server))
	sw := make(HostSwitch)
	for name, config := range configs.Server {
		if dupe[config.Address] {
			panic("Duplicate Address Detected: " + config.Address)
		}
		server := New(config)
		if !ValidateHostPort(server.Server.Addr, true) {
			panic("invalid HTTP server listener address")
		}
		_, hp := getHostPort(server.Server.Addr)
		hostPorts[hp] = append(hostPorts[hp], server.Server.Addr)
		if len(hostPorts[hp]) > 1 {
			useHS = true
		}
		servers[name] = server
		sw[server.Server.Addr] = server.Router
	}
	if useHS {
		for p := range hostPorts {
			if len(hostPorts[p]) > 1 {
				switchPorts = append(switchPorts, p)
				for _, address := range hostPorts[p] {
					for name := range servers {
						if servers[name].Server.Addr == address {
							hsMap[name] = true
							servers[name].Server.Handler = sw
						}
					}
				}
			}
		}
	}
	return &Module{
		Servers:     servers,
		Configs:     configs,
		Switch:      sw,
		SwitchPorts: switchPorts,
		useHS:       useHS,
		hsMap:       hsMap,
	}
}

// HostSwitch allows mapping of specific host addresses to Handlers.
type HostSwitch map[string]http.Handler

// ServeHTTP Implement the method on the HostSwitch.
func (hs HostSwitch) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check if a http.Handler is registered for the given host.
	// If yes, use it to handle the request.
	if handler := hs[r.Host]; handler != nil {
		handler.ServeHTTP(w, r)
	} else {
		// Handle host names for which no handler is registered
		http.Error(w, "Forbidden", 403) // Or Redirect?
	}
}

// GET adds a Get Request to all HTTPServers.
func (m *Module) GET(path string, handle httprouter.Handle) {
	for name := range m.Servers {
		m.Servers[name].Router.GET(path, handle)
	}
}

// HostGET adds a Get Request to the specified named HTTPServer.
func (m *Module) HostGET(host, path string, handle httprouter.Handle) {
	m.Servers[host].Router.GET(path, handle)
}

// Configure is called to configure the HTTP server. This includes validating all configurations for each configured
// listener (which are not treated as separate modules, as opposed to other coordinators), as well as setting up the
// request router. Any configuration failure will cause the func to panic with an appropriate error message.
//
// If no listener has been configured, the coordinator will set up a default listener on a random port greater than
// 1024, as selected by the net.Listener call. This listener will be logged so that the port chosen will be known.
func (m *Module) Configure() {

	m.Logger = configureLogger(LogLevel)
	m.Logger.Info("configuring HTTPServers")

	if len(m.Servers) == 0 {
		panic("No HTTPServers Defined")
	}

	// Change to random ports for members of HostSwitch:
	for name, server := range m.Servers {
		if m.hsMap[name] {
			hostname, _ := getHostPort(server.Server.Addr)
			server.Server.Addr = hostname + ":"
		}
		if server.Config.Timeout > m.MaxTimeout {
			m.MaxTimeout = server.Config.Timeout
		}
	}
}

// Start is responsible for starting the listener on each configured address. If any listener fails to start, the error
// is logged, and the listeners that have already been started are stopped. The func then returns the error encountered
// to the caller. Once the listeners are all started, the HTTP server itself is started on each listener to respond to
// requests.
func (m *Module) Start() error {
	m.Logger.Info("starting")
	// Start listeners
	listeners := make(map[string]net.Listener)
	for name := range m.Servers {
		ln, err := net.Listen("tcp", m.Servers[name].Server.Addr)
		if err != nil {
			m.Logger.Error("failed to listen", zap.String("listener", m.Servers[name].Server.Addr), zap.Error(err))
			for _, listenerToClose := range listeners {
				if listenerToClose != nil {
					closeErr := listenerToClose.Close()
					if closeErr != nil {
						m.Logger.Error("could not close listener: %v", zap.Error(closeErr))
					}
				}
			}
			return err
		}

		m.Logger.Info("started listener", zap.String("listener", ln.Addr().String()))
		listeners[name] = tcpKeepAliveListener{
			Keepalive:   m.Servers[name].Server.IdleTimeout,
			TCPListener: ln.(*net.TCPListener),
		}
	}

	for _, port := range m.SwitchPorts {
		m.Logger.Info("started listener", zap.String("hostswitch listener", ":"+port))
		go http.ListenAndServe(":"+port, m.Switch)
	}

	for name, server := range m.Servers {
		if server.Config.CertFile != "" || server.Config.KeyFile != "" {
			go server.Server.ServeTLS(listeners[name], server.Config.CertFile, server.Config.KeyFile)
		} else {
			go server.Server.Serve(listeners[name])
		}
	}
	return nil
}

// Stop calls the Close func for each configured HTTP server listener. This stops the underlying HTTP server without
// waiting for client calls to complete. If there are any errors while shutting down the listeners, this does not stop
// other listeners from being closed. A generic error will be returned to the caller in this case.
func (m *Module) Stop() error {
	m.Logger.Info("shutdown")

	// Close all servers
	collectedErrors := make([]zapcore.Field, 0)
	for _, server := range m.Servers {
		err := server.Server.Close()
		if err != nil {
			collectedErrors = append(collectedErrors, zap.Error(err))
		}
	}

	if len(collectedErrors) > 0 {
		m.Logger.Error("errors shutting down", collectedErrors...)
		return errors.New("error shutting down HTTP servers")
	}
	return nil
}

func makeRequestInfo(r *http.Request) httpResponseRequestInfo {
	hostname, _ := os.Hostname()
	return httpResponseRequestInfo{
		URI:  r.URL.Path,
		Host: hostname,
	}
}

func (m *Module) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, jsonObj interface{}) {
	// Add CORS header, if configured
	corsHeader := viper.GetString("general.access-control-allow-origin")
	if corsHeader != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsHeader)
	}

	w.Header().Set("Content-Type", "application/json")

	if jsonBytes, err := json.Marshal(jsonObj); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"error\":true,\"message\":\"could not encode JSON\",\"result\":{}}"))
	} else {
		w.WriteHeader(statusCode)
		w.Write(jsonBytes)
	}
}

func (m *Module) writeErrorResponse(w http.ResponseWriter, r *http.Request, errValue int, message string) {
	m.writeResponse(w, r, errValue, httpResponseError{
		Error:   true,
		Message: message,
		Request: makeRequestInfo(r),
	})
}

// This is a catch-all handler for unknown URLs. It should return a 404
type defaultHandler struct{}

func (handler *defaultHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "{\"error\":true,\"message\":\"invalid request type\",\"result\":{}}", http.StatusNotFound)
}

func (m *Module) handleAdmin(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Add CORS header, if configured
	corsHeader := viper.GetString("general.access-control-allow-origin")
	if corsHeader != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsHeader)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("GOOD"))
}

/// Helpers:

// ValidateIP returns true if the provided string can be parsed as an IP address (either IPv4 or IPv6).
func ValidateIP(ipaddr string) bool {
	addr := net.ParseIP(ipaddr)
	return addr != nil
}

// ValidateHostPort returns true if the provided string is of the form "hostname:port", where hostname is a valid
// hostname or IP address (as parsed by ValidateIP or ValidateHostname), and port is a valid integer.
func ValidateHostPort(host string, allowBlankHost bool) bool {
	// Must be hostname:port, ipv4:port, or [ipv6]:port. Optionally allow blank hostname
	hostname, portString, err := net.SplitHostPort(host)
	if err != nil {
		return false
	}

	// Validate the port is a numeric (yeah, strings are valid in some places, but we don't support it)
	_, err = strconv.Atoi(portString)
	if err != nil {
		return false
	}

	// Listeners can have blank hostnames, so we'll skip validation if that's what we're looking for
	if allowBlankHost && hostname == "" {
		return true
	}

	// Only IPv6 can contain :
	if strings.Contains(hostname, ":") && (!ValidateIP(hostname)) {
		return false
	}

	// If all the parts of the hostname are numbers, validate as IP. Otherwise, it's a hostname
	hostnameParts := strings.Split(hostname, ".")
	isIP4 := true
	for _, section := range hostnameParts {
		_, err := strconv.Atoi(section)
		if err != nil {
			isIP4 = false
			break
		}
	}
	if isIP4 {
		return ValidateIP(hostname)
	}
	return ValidateHostname(hostname)
}

func getHostPort(host string) (string, string) {
	hostString, portString, err := net.SplitHostPort(host)
	if err != nil {
		panic("Could not determine host port: " + err.Error())
	}
	return hostString, portString
}

// ValidateHostname returns true if the provided string can be parsed as a hostname. In general this means:
//
// * One or more segments delimited by a '.'
// * Each segment can be no more than 63 characters long
// * Valid characters in a segment are letters, numbers, and dashes
// * Segments may not start or end with a dash
// * The exception is IPv6 addresses, which are also permitted.
func ValidateHostname(hostname string) bool {
	matches, _ := regexp.MatchString(`^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*$`, hostname)
	if !matches {
		// Try as an IP address
		return ValidateIP(hostname)
	}
	return matches
}

func configureLogger(logLevel string) *zap.Logger {
	var level zap.AtomicLevel
	var syncOutput zapcore.WriteSyncer
	switch strings.ToLower(logLevel) {
	case "none":
		return zap.NewNop()
	case "", "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	case "panic":
		level = zap.NewAtomicLevelAt(zap.PanicLevel)
	case "fatal":
		level = zap.NewAtomicLevelAt(zap.FatalLevel)
	default:
		fmt.Printf("Invalid log level supplied. Defaulting to info: %s", logLevel)
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	syncOutput = zapcore.Lock(os.Stdout)
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		syncOutput,
		level,
	)
	logger := zap.New(core)
	//zap.ReplaceGlobals(logger)
	return logger
}
