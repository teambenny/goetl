package processors

import (
	"io"
	"io/ioutil"
	"net/http"

	"github.com/teambenny/goetl/etldata"
	"github.com/teambenny/goetl/etlutil"
)

// HTTPRequest executes an HTTP request and passes along the response body.
// It is simply wrapping an http.Request and http.Client object. See the
// net/http docs for more info: https://golang.org/pkg/net/http
type HTTPRequest struct {
	Request *http.Request
	Client  *http.Client
}

// NewHTTPRequest creates a new HTTPRequest and is essentially wrapping net/http's NewRequest
// function. See https://golang.org/pkg/net/http/#NewRequest
func NewHTTPRequest(method, url string, body io.Reader) (*HTTPRequest, error) {
	req, err := http.NewRequest(method, url, body)
	return &HTTPRequest{Request: req, Client: &http.Client{}}, err
}

// ProcessData sends data to outputChan if the response body is not null
func (r *HTTPRequest) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	resp, err := r.Client.Do(r.Request)
	etlutil.KillPipelineIfErr(err, killChan)
	if resp != nil && resp.Body != nil {
		dd, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		etlutil.KillPipelineIfErr(err, killChan)
		outputChan <- etldata.JSON(dd)
	}
}

// Finish - see interface for documentation.
func (r *HTTPRequest) Finish(outputChan chan etldata.Payload, killChan chan error) {
}

func (r *HTTPRequest) String() string {
	return "HTTPRequest"
}
