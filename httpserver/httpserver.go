package httpserver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
)

// Config contains detailed settings for a httpserver.
type Config struct {
	Name     string
	Address  string
	CertFile string
	KeyFile  string
	CAFile   string
	NoVerify bool
	Timeout  int

	// Future implement individual timeouts:
	//ReadTimeout       time.Duration
	//ReadHeaderTimeout time.Duration
	//WriteTimeout      time.Duration
	//IdleTimeout       time.Duration
}

// NewConfig returns a Config with defaults.
func NewConfig() *Config {
	return &Config{
		Name:     "default",
		Address:  ":0",
		Timeout:  30,
		NoVerify: true,
	}
}

// HTTPServer contains the components for a HTTP Server.
type HTTPServer struct {
	Name   string
	Router *httprouter.Router
	Server *http.Server
	Config *Config
}

// New returns a HTTPServer using the given Config.
// If no Config is provided, then the default Config is used.
func New(config *Config) *HTTPServer {
	if config == nil {
		config = NewConfig()
	}
	return configureHTTPServer(config)
}

func configureHTTPServer(config *Config) *HTTPServer {
	server := HTTPServer{
		Name:   config.Name,
		Router: httprouter.New(),
		Config: config,
	}
	timeout := time.Duration(config.Timeout) * time.Second
	server.Server = &http.Server{
		Handler:           server.Router,
		Addr:              config.Address,
		ReadTimeout:       timeout,
		ReadHeaderTimeout: timeout,
		IdleTimeout:       timeout,
	}
	if config.CAFile != "" {
		caCert, err := ioutil.ReadFile(config.CAFile)
	TLSConfig:
		switch {
		case err != nil:
			fmt.Println("ERROR:", "cannot read TLS CA file:", err)
			fmt.Println("Skipping TLSConfig")
			break TLSConfig
		case config.KeyFile == "", config.CertFile == "":
			fmt.Println("ERROR: TLS HTTP server specified with missing certificate or key")
			fmt.Println("Skipping TLSConfig")
			break TLSConfig
		default:
			cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if err != nil {
				//panic("cannot read TLS certificate or key file: " + err.Error())
				fmt.Println("cannot read TLS certificate or key file: " + err.Error())
				fmt.Println("Skipping TLSConfig")
				break TLSConfig
			}
			server.Server.TLSConfig = &tls.Config{
				InsecureSkipVerify: config.NoVerify,
			}
			server.Server.TLSConfig.RootCAs = x509.NewCertPool()
			server.Server.TLSConfig.RootCAs.AppendCertsFromPEM(caCert)
			server.Server.TLSConfig.Certificates = []tls.Certificate{cert}
			server.Server.TLSConfig.BuildNameToCertificate()
		}
	}
	return &server
}

// GET adds a Handler for the specified path.
// Shortcut for router.Handle("GET", path, handle)
func (s *HTTPServer) GET(path string, handle httprouter.Handle) {
	s.Router.GET(path, handle)
}

// POST adds a Handler for the specified path.
// Shortcut for router.Handle("POST", path, handle)
func (s *HTTPServer) POST(path string, handle httprouter.Handle) {
	s.Router.POST(path, handle)
}

// Serve starts the HTTP server and listens.
func (s *HTTPServer) Serve() error {
	ln, err := net.Listen("tcp", s.Server.Addr)
	if err != nil {
		if ln != nil {
			closeErr := ln.Close()
			if closeErr != nil {
				fmt.Println("Error closing listener:", closeErr)
			}
		}
		return err
	}
	listener := tcpKeepAliveListener{
		Keepalive:   s.Server.IdleTimeout,
		TCPListener: ln.(*net.TCPListener),
	}
	fmt.Println("starting listener", ln.Addr().String())
	if s.Config.CertFile != "" || s.Config.KeyFile != "" {
		return s.Server.ServeTLS(listener, s.Config.CertFile, s.Config.KeyFile)
	}
	return s.Server.Serve(listener)
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
