package httpserver

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jbvmio/modules/coop"
	"github.com/julienschmidt/httprouter"
	"github.com/spf13/viper"
	"go.uber.org/zap/zapcore"

	"go.uber.org/zap"
)

const (
	moduleName  = `httpserver`
	moduleClass = `http`
)

// HTTPServerModule runs the HTTP interface for Burrow, managing all configured listeners.
type HTTPServerModule struct {
	// App is a pointer to the application context. This stores the channel to the storage subsystem
	App *coop.ApplicationContext

	// Log is a logger that has been configured for this module to use. Normally, this means it has been set up with
	// fields that are appropriate to identify this coordinator
	Log *zap.Logger

	router  *httprouter.Router
	servers map[string]*http.Server
	theCert map[string]string
	theKey  map[string]string
}

// Configure is called to configure the HTTP server. This includes validating all configurations for each configured
// listener (which are not treated as separate modules, as opposed to other coordinators), as well as setting up the
// request router. Any configuration failure will cause the func to panic with an appropriate error message.
//
// If no listener has been configured, the coordinator will set up a default listener on a random port greater than
// 1024, as selected by the net.Listener call. This listener will be logged so that the port chosen will be known.
func (module *HTTPServerModule) Configure() {
	module.Log.Info("configuring")
	module.router = httprouter.New()

	// If no HTTP server configured, add a default HTTP server that listens on a random port
	servers := viper.GetStringMap("httpserver")
	if len(servers) == 0 {
		viper.Set("httpserver.default.address", ":0")
		servers = viper.GetStringMap("httpserver")
	}

	// Validate provided HTTP server configs
	module.servers = make(map[string]*http.Server)
	module.theCert = make(map[string]string)
	module.theKey = make(map[string]string)
	for name := range servers {
		configRoot := "httpserver." + name
		server := &http.Server{
			Handler: module.router,
		}

		server.Addr = viper.GetString(configRoot + ".address")
		if !ValidateHostPort(server.Addr, true) {
			panic("invalid HTTP server listener address")
		}

		viper.SetDefault(configRoot+".timeout", 300)
		timeout := viper.GetInt(configRoot + ".timeout")
		server.ReadTimeout = time.Duration(timeout) * time.Second
		server.ReadHeaderTimeout = time.Duration(timeout) * time.Second
		server.WriteTimeout = time.Duration(timeout) * time.Second
		server.IdleTimeout = time.Duration(timeout) * time.Second
		keyFile := ""
		certFile := ""
		if viper.IsSet(configRoot + ".tls") {
			tlsName := viper.GetString(configRoot + ".tls")
			certFile = viper.GetString("tls." + tlsName + ".certfile")
			keyFile = viper.GetString("tls." + tlsName + ".keyfile")
			caFile := viper.GetString("tls." + tlsName + ".cafile")

			server.TLSConfig = &tls.Config{}

			if caFile != "" {
				caCert, err := ioutil.ReadFile(caFile)
				if err != nil {
					panic("cannot read TLS CA file: " + err.Error())
				}
				server.TLSConfig.RootCAs = x509.NewCertPool()
				server.TLSConfig.RootCAs.AppendCertsFromPEM(caCert)
			}

			if certFile == "" || keyFile == "" {
				panic("TLS HTTP server specified with missing certificate or key")
			}
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				panic("cannot read TLS certificate or key file: " + err.Error())
			}
			server.TLSConfig.Certificates = []tls.Certificate{cert}
			server.TLSConfig.BuildNameToCertificate()
		}
		module.servers[name] = server
		module.theCert[name] = certFile
		module.theKey[name] = keyFile
	}

	// Configure URL routes here

	// This is a catchall for undefined URLs
	module.router.NotFound = &defaultHandler{}

	// This is a healthcheck URL. Please don't change it
	module.router.GET("/burrow/admin", module.handleAdmin)

	// All valid paths go here
	/*
		module.router.GET("/v3/kafka", module.handleClusterList)
		module.router.GET("/v3/kafka/:cluster", module.handleClusterDetail)
		module.router.GET("/v3/kafka/:cluster/topic", module.handleTopicList)
		module.router.GET("/v3/kafka/:cluster/topic/:topic", module.handleTopicDetail)
		module.router.GET("/v3/kafka/:cluster/topic/:topic/consumers", module.handleTopicConsumerList)
		module.router.GET("/v3/kafka/:cluster/consumer", module.handleConsumerList)
		module.router.GET("/v3/kafka/:cluster/consumer/:consumer", module.handleConsumerDetail)
		module.router.GET("/v3/kafka/:cluster/consumer/:consumer/status", module.handleConsumerStatus)
		module.router.GET("/v3/kafka/:cluster/consumer/:consumer/lag", module.handleConsumerStatusComplete)

		module.router.GET("/v3/config", module.configMain)
		module.router.GET("/v3/config/storage", module.configStorageList)
		module.router.GET("/v3/config/storage/:name", module.configStorageDetail)
		module.router.GET("/v3/config/evaluator", module.configEvaluatorList)
		module.router.GET("/v3/config/evaluator/:name", module.configEvaluatorDetail)
		module.router.GET("/v3/config/cluster", module.configClusterList)
		module.router.GET("/v3/config/cluster/:cluster", module.handleClusterDetail)
		module.router.GET("/v3/config/consumer", module.configConsumerList)
		module.router.GET("/v3/config/consumer/:name", module.configConsumerDetail)
		module.router.GET("/v3/config/notifier", module.configNotifierList)
		module.router.GET("/v3/config/notifier/:name", module.configNotifierDetail)
	*/

	// TODO: This should really have authentication protecting it
	//module.router.DELETE("/v3/kafka/:cluster/consumer/:consumer", module.handleConsumerDelete)
	module.router.GET("/v3/admin/loglevel", module.getLogLevel)
	module.router.POST("/v3/admin/loglevel", module.setLogLevel)
}

// Start is responsible for starting the listener on each configured address. If any listener fails to start, the error
// is logged, and the listeners that have already been started are stopped. The func then returns the error encountered
// to the caller. Once the listeners are all started, the HTTP server itself is started on each listener to respond to
// requests.
func (module *HTTPServerModule) Start() error {
	module.Log.Info("starting")

	// Start listeners
	listeners := make(map[string]net.Listener)

	for name, server := range module.servers {
		ln, err := net.Listen("tcp", module.servers[name].Addr)
		if err != nil {
			module.Log.Error("failed to listen", zap.String("listener", module.servers[name].Addr), zap.Error(err))
			for _, listenerToClose := range listeners {
				if listenerToClose != nil {
					closeErr := listenerToClose.Close()
					if closeErr != nil {
						module.Log.Error("could not close listener: %v", zap.Error(closeErr))
					}
				}
			}
			return err
		}
		module.Log.Info("started listener", zap.String("listener", ln.Addr().String()))
		listeners[name] = tcpKeepAliveListener{
			Keepalive:   server.IdleTimeout,
			TCPListener: ln.(*net.TCPListener),
		}
	}

	// Start the HTTP server on the listeners
	for name, server := range module.servers {
		if module.theCert[name] != "" || module.theKey[name] != "" {
			go server.ServeTLS(listeners[name], module.theCert[name], module.theKey[name])
		} else {
			go server.Serve(listeners[name])
		}

	}
	return nil
}

// Stop calls the Close func for each configured HTTP server listener. This stops the underlying HTTP server without
// waiting for client calls to complete. If there are any errors while shutting down the listeners, this does not stop
// other listeners from being closed. A generic error will be returned to the caller in this case.
func (module *HTTPServerModule) Stop() error {
	module.Log.Info("shutdown")

	// Close all servers
	collectedErrors := make([]zapcore.Field, 0)
	for _, server := range module.servers {
		err := server.Close()
		if err != nil {
			collectedErrors = append(collectedErrors, zap.Error(err))
		}
	}

	if len(collectedErrors) > 0 {
		module.Log.Error("errors shutting down", collectedErrors...)
		return errors.New("error shutting down HTTP servers")
	}
	return nil
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted connections. It's used by ListenAndServe and
// ListenAndServeTLS so dead TCP connections (e.g. closing laptop mid-download) eventually go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
	Keepalive time.Duration
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}

	if ln.Keepalive > 0 {
		tc.SetKeepAlive(true)
		tc.SetKeepAlivePeriod(ln.Keepalive)
	}
	return tc, nil
}

func makeRequestInfo(r *http.Request) httpResponseRequestInfo {
	hostname, _ := os.Hostname()
	return httpResponseRequestInfo{
		URI:  r.URL.Path,
		Host: hostname,
	}
}

func (module *HTTPServerModule) writeResponse(w http.ResponseWriter, r *http.Request, statusCode int, jsonObj interface{}) {
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

func (module *HTTPServerModule) writeErrorResponse(w http.ResponseWriter, r *http.Request, errValue int, message string) {
	module.writeResponse(w, r, errValue, httpResponseError{
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

func (module *HTTPServerModule) handleAdmin(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Add CORS header, if configured
	corsHeader := viper.GetString("general.access-control-allow-origin")
	if corsHeader != "" {
		w.Header().Set("Access-Control-Allow-Origin", corsHeader)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("GOOD"))
}

func (module *HTTPServerModule) getLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	requestInfo := makeRequestInfo(r)
	module.writeResponse(w, r, http.StatusOK, httpResponseLogLevel{
		Error:   false,
		Message: "log level returned",
		Level:   module.App.LogLevel.Level().String(),
		Request: requestInfo,
	})
}

func (module *HTTPServerModule) setLogLevel(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Decode the JSON body
	decoder := json.NewDecoder(r.Body)
	var req logLevelRequest
	err := decoder.Decode(&req)
	if err != nil {
		module.writeErrorResponse(w, r, http.StatusBadRequest, "could not decode message body")
		return
	}
	r.Body.Close()

	// Explicitly validate the log level provided
	switch strings.ToLower(req.Level) {
	case "debug", "trace":
		module.App.LogLevel.SetLevel(zap.DebugLevel)
	case "info":
		module.App.LogLevel.SetLevel(zap.InfoLevel)
	case "warning", "warn":
		module.App.LogLevel.SetLevel(zap.WarnLevel)
	case "error":
		module.App.LogLevel.SetLevel(zap.ErrorLevel)
	case "fatal":
		module.App.LogLevel.SetLevel(zap.FatalLevel)
	default:
		module.writeErrorResponse(w, r, http.StatusNotFound, "unknown log level")
		return
	}

	requestInfo := makeRequestInfo(r)
	module.writeResponse(w, r, http.StatusOK, httpResponseError{
		Error:   false,
		Message: "set log level",
		Request: requestInfo,
	})
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
