package relay

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/services/httpd"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"encoding/json"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"net/http/httptest"
)

// HTTP is a relay for HTTP influxdb writes
type HTTP struct {
	addr   string
	name   string
	schema string

	cert string
	rp   string

	closing int64
	l       net.Listener

	backends []*httpBackend
}

const (
	DefaultHTTPTimeout      = 10 * time.Second
	DefaultMaxDelayInterval = 10 * time.Second
	DefaultBatchSizeKB      = 512

	KB = 1024
	MB = 1024 * KB
)

func NewHTTP(cfg HTTPConfig) (Relay, error) {
	h := new(HTTP)

	h.addr = cfg.Addr
	h.name = cfg.Name

	h.cert = cfg.SSLCombinedPem
	h.rp = cfg.DefaultRetentionPolicy

	h.schema = "http"
	if h.cert != "" {
		h.schema = "https"
	}

	for i := range cfg.Outputs {
		backend, err := newHTTPBackend(&cfg.Outputs[i])
		if err != nil {
			return nil, err
		}

		h.backends = append(h.backends, backend)
	}

	for _, b := range h.backends {
		newHealthChecker(b).startHealthChecker()
	}

	return h, nil
}

func (h *HTTP) Name() string {
	if h.name == "" {
		return fmt.Sprintf("%s://%s", h.schema, h.addr)
	}
	return h.name
}

func (h *HTTP) Run() error {
	l, err := net.Listen("tcp", h.addr)
	if err != nil {
		return err
	}

	// support HTTPS
	if h.cert != "" {
		cert, err := tls.LoadX509KeyPair(h.cert, h.cert)
		if err != nil {
			return err
		}

		l = tls.NewListener(l, &tls.Config{
			Certificates: []tls.Certificate{cert},
		})
	}

	h.l = l

	log.Printf("Starting %s relay %q on %v", strings.ToUpper(h.schema), h.Name(), h.addr)

	err = http.Serve(l, h)
	if atomic.LoadInt64(&h.closing) != 0 {
		return nil
	}
	return err
}

func (h *HTTP) Stop() error {
	atomic.StoreInt64(&h.closing, 1)
	return h.l.Close()
}

func drainBody(body io.ReadCloser) (bf *bytes.Reader, err error) {
	if body == nil {
		return bytes.NewReader(make([]byte, 1)), nil
	}
	var buf = new(bytes.Buffer)
	if _, err = buf.ReadFrom(body); err != nil {
		return bytes.NewReader(buf.Bytes()), err
	}
	if err = body.Close(); err != nil {
		return bytes.NewReader(buf.Bytes()), err
	}
	return bytes.NewReader(buf.Bytes()), nil
}

func (h *HTTP) serveQuery(w http.ResponseWriter, r *http.Request) {
	rw, ok := w.(httpd.ResponseWriter)
	if !ok {
		rw = httpd.NewResponseWriter(w, r)
	}
	bodyReader, err := drainBody(r.Body)
	if err != nil {
		h.httpError(rw, err.Error(), http.StatusBadGateway)
		return
	}
	r.Body = ioutil.NopCloser(bodyReader)

	var qr io.Reader
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		qr = strings.NewReader(qp)
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				h.httpError(rw, err.Error(), http.StatusBadRequest)
				return
			}
			defer f.Close()
			qr = f
		}
	}

	if qr == nil {
		h.httpError(rw, `missing required parameter "q"`, http.StatusBadRequest)
		return
	}

	p := influxql.NewParser(qr)
	db := r.FormValue("db")

	// Parse query from query string.
	query, err := p.ParseQuery()
	if err != nil {
		h.httpError(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	sts := query.Statements
	resp := httpd.Response{Results: make([]*influxql.Result, 0)}

	if len(sts) == 0 {
		rw.WriteResponse(resp)
		return
	}

	if len(sts) == 1 {
		st := sts[0]
		switch st.(type) {
		case *influxql.ShowDatabasesStatement:
			databases := map[string]bool{}
			for _, b := range h.backends {
				for k, v := range b.databases {
					databases[k] = v
				}
			}
			values := [][]interface{}{}
			for k, _ := range databases {
				values = append(values, []interface{}{k})
			}

			rows := models.Rows{}
			resp.Results = append(resp.Results, &influxql.Result{
				StatementID: 0,
				Series: append(rows, &models.Row{
					Name:    "databases",
					Columns: []string{"name"},
					Values:  values,
				}),
			})
			rw.WriteResponse(resp)
			return
		}
	}

	if db == "" {
		for i, _ := range sts {
			resp.Results = append(resp.Results, &influxql.Result{
				StatementID: i,
				Err:         errors.New("database name required"),
			})
		}
		rw.WriteResponse(resp)
		return
	}

	for _, st := range sts {
		_, ok := st.(*influxql.SelectStatement)
		if !ok {
			switch st.(type) {
			case *influxql.ShowMeasurementsStatement:
				continue
			case *influxql.ShowTagValuesStatement:
				continue
			case *influxql.ShowTagKeysStatement:
				continue
			case *influxql.ShowSeriesStatement:
				continue
			default:
				h.httpError(rw, "contains unsupported statement", http.StatusBadRequest)
				return
			}
		}
	}

	for _, b := range h.backends {
		if b.acceptDb(db) && b.alive {
			bodyReader.Seek(0, 0)
			b.reverseProxy.ServeHTTP(w, r)
			return
		}
	}

	h.httpError(rw, "no available backend", http.StatusBadGateway)
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if r.URL.Path == "/ping" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Add("X-InfluxDB-Version", "relay")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.URL.Path == "/ok.htm" || r.URL.Path == "/ok" {
		w.Write([]byte("ok"))
		return
	}

	if r.URL.Path == "/health" {
		for _, b := range h.backends {
			w.Write([]byte(fmt.Sprintf("%s is %s\n", b.name, ifelse(b.alive, "up", "down"))))
		}
		return
	}

	if r.URL.Path == "/query" {
		h.serveQuery(w, r)
		return
	}

	if r.URL.Path != "/write" {
		jsonError(w, http.StatusNotFound, "invalid write endpoint")
		return
	}

	if r.Method != "POST" {
		w.Header().Set("Allow", "POST")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
		} else {
			jsonError(w, http.StatusMethodNotAllowed, "invalid write method")
		}
		return
	}

	queryParams := r.URL.Query()
	// fail early if we're missing the database
	db := queryParams.Get("db")
	if db == "" {
		jsonError(w, http.StatusBadRequest, "missing parameter: \"db\"")
		return
	}

	if queryParams.Get("rp") == "" && h.rp != "" {
		queryParams.Set("rp", h.rp)
	}

	var body = r.Body

	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			jsonError(w, http.StatusBadRequest, "unable to decode gzip body")
		}
		defer b.Close()
		body = b
	}

	bodyBuf := getBuf()
	_, err := bodyBuf.ReadFrom(body)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusInternalServerError, "problem reading request body")
		return
	}

	precision := queryParams.Get("precision")
	points, err := models.ParsePointsWithPrecision(bodyBuf.Bytes(), start, precision)
	if err != nil {
		putBuf(bodyBuf)
		jsonError(w, http.StatusBadRequest, "unable to parse points")
		return
	}

	outBuf := getBuf()
	for _, p := range points {
		if _, err = outBuf.WriteString(p.PrecisionString(precision)); err != nil {
			break
		}
		if err = outBuf.WriteByte('\n'); err != nil {
			break
		}
	}

	// done with the input points
	putBuf(bodyBuf)

	if err != nil {
		putBuf(outBuf)
		jsonError(w, http.StatusInternalServerError, "problem writing points")
		return
	}

	// normalize query string
	query := queryParams.Encode()

	outBytes := outBuf.Bytes()

	// check for authorization performed via the header
	authHeader := r.Header.Get("Authorization")

	var wg sync.WaitGroup
	wg.Add(len(h.backends))

	var responses = make(chan *responseData, len(h.backends))

	for _, b := range h.backends {
		b := b
		go func() {
			defer wg.Done()
			if b.acceptDb(db) {
				resp, err := b.post(outBytes, query, authHeader)
				if err != nil {
					log.Printf("Problem posting to relay %q backend %q: %v", h.Name(), b.name, err)
				} else {
					if resp.StatusCode/100 == 5 {
						log.Printf("5xx response for relay %q backend %q: %v", h.Name(), b.name, resp.StatusCode)
					}
					responses <- resp
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
		putBuf(outBuf)
	}()

	var errResponse *responseData

	for resp := range responses {
		switch resp.StatusCode / 100 {
		case 2:
			w.WriteHeader(http.StatusNoContent)
			return

		case 4:
			// user error
			resp.Write(w)
			return

		default:
			// hold on to one of the responses to return back to the client
			errResponse = resp
		}
	}

	// no successful writes
	if errResponse == nil {
		// failed to make any valid request...
		jsonError(w, http.StatusServiceUnavailable, "unable to write points")
		return
	}

	errResponse.Write(w)
}

type responseData struct {
	ContentType     string
	ContentEncoding string
	StatusCode      int
	Body            []byte
}

func (rd *responseData) Write(w http.ResponseWriter) {
	if rd.ContentType != "" {
		w.Header().Set("Content-Type", rd.ContentType)
	}

	if rd.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", rd.ContentEncoding)
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(rd.Body)))
	w.WriteHeader(rd.StatusCode)
	w.Write(rd.Body)
}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	data := fmt.Sprintf("{\"error\":%q}\n", message)
	w.Header().Set("Content-Length", fmt.Sprint(len(data)))
	w.WriteHeader(code)
	w.Write([]byte(data))
}

// httpError writes an error to the client in a standard format.
func (h *HTTP) httpError(w http.ResponseWriter, error string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", "influxdb"))
	}

	response := httpd.Response{Err: errors.New(error)}
	if rw, ok := w.(httpd.ResponseWriter); ok {
		h.writeHeader(w, code)
		rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	b, _ := json.Marshal(response)
	w.Write(b)
}

func (h *HTTP) writeHeader(w http.ResponseWriter, code int) {
	w.WriteHeader(code)
}

type poster interface {
	post([]byte, string, string) (*responseData, error)
}

type simplePoster struct {
	client   *http.Client
	location string
}

func newSimplePoster(location string, timeout time.Duration, skipTLSVerification bool) *simplePoster {
	// Configure custom transport for http.Client
	// Used for support skip-tls-verification option
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: skipTLSVerification,
		},
	}

	return &simplePoster{
		client: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		location: location,
	}
}

func (b *simplePoster) post(buf []byte, query string, auth string) (*responseData, error) {
	req, err := http.NewRequest("POST", b.location, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}

	req.URL.RawQuery = query
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Content-Length", strconv.Itoa(len(buf)))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	return &responseData{
		ContentType:     resp.Header.Get("Conent-Type"),
		ContentEncoding: resp.Header.Get("Conent-Encoding"),
		StatusCode:      resp.StatusCode,
		Body:            data,
	}, nil
}

type httpBackend struct {
	poster
	name         string
	databases    map[string]bool
	reverseProxy *httputil.ReverseProxy
	alive        bool
}

func newHTTPBackend(cfg *HTTPOutputConfig) (*httpBackend, error) {
	if cfg.Name == "" {
		cfg.Name = cfg.Location
	}

	databases := map[string]bool{}

	for _, db := range cfg.Databases {
		databases[db] = true
	}

	timeout := DefaultHTTPTimeout
	if cfg.Timeout != "" {
		t, err := time.ParseDuration(cfg.Timeout)
		if err != nil {
			return nil, fmt.Errorf("error parsing HTTP timeout '%v'", err)
		}
		timeout = t
	}

	var p poster = newSimplePoster(cfg.Location, timeout, cfg.SkipTLSVerification)

	// If configured, create a retryBuffer per backend.
	// This way we serialize retries against each backend.
	if cfg.BufferSizeMB > 0 {
		max := DefaultMaxDelayInterval
		if cfg.MaxDelayInterval != "" {
			m, err := time.ParseDuration(cfg.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}

		batch := DefaultBatchSizeKB * KB
		if cfg.MaxBatchKB > 0 {
			batch = cfg.MaxBatchKB * KB
		}

		p = newRetryBuffer(cfg.BufferSizeMB*MB, batch, max, p)
	}

	u, error := url.Parse(cfg.Location)
	if error != nil {
		return nil, error
	}
	u.Path = ""

	return &httpBackend{
		poster:       p,
		name:         cfg.Name,
		databases:    databases,
		reverseProxy: httputil.NewSingleHostReverseProxy(u),
		alive:        true,
	}, nil
}

func (h *httpBackend) acceptDb(db string) bool {
	return h.databases[db]
}

var ErrBufferFull = errors.New("retry buffer full")

var bufPool = sync.Pool{New: func() interface{} {
	return new(bytes.Buffer)
}}

func getBuf() *bytes.Buffer {
	if bb, ok := bufPool.Get().(*bytes.Buffer); ok {
		return bb
	}
	return new(bytes.Buffer)
}

func putBuf(b *bytes.Buffer) {
	b.Reset()
	bufPool.Put(b)
}

type healthChecker struct {
	backend      *httpBackend
	fails        int
	interval     time.Duration
	failsTimeout time.Duration
}

func newHealthChecker(backend *httpBackend) *healthChecker {
	return &healthChecker{
		backend:      backend,
		fails:        1,
		interval:     5 * time.Second,
		failsTimeout: 10 * time.Second,
	}
}

func (c *healthChecker) startHealthChecker() {
	fails := 0
	go func() {
		for true {
			req := httptest.NewRequest("GET", "/ping", nil)
			w := httptest.NewRecorder()
			c.backend.reverseProxy.ServeHTTP(w, req)
			resp := w.Result()
			if resp.StatusCode/100 != 2 {
				fails += 1
			}
			if fails >= c.fails {
				if c.backend.alive {
					log.Printf("backend %s is down", c.backend.name)
				}
				c.backend.alive = false
				time.Sleep(c.failsTimeout)
				fails = c.fails - 1
			} else {
				fails = 0
				if !c.backend.alive {
					log.Printf("backend %s is up", c.backend.name)
				}
				c.backend.alive = true
				time.Sleep(c.interval)
			}
		}
	}()
}

func ifelse(c bool, t string, f string) string {
	if c {
		return t
	} else {
		return f
	}
}
