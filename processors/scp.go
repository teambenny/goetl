package processors

import (
	"fmt"
	"os/exec"

	"github.com/will-beep-lamm/goetl/etldata"
	"github.com/will-beep-lamm/goetl/etlutil"
)

// SCP executes the scp command, sending the given file to the given destination.
type SCP struct {
	Port        string // e.g., "2222" -- only send for non-standard ports
	Object      string // e.g., "/path/to/file.txt"
	Destination string // e.g., "user@host:/path/to/destination/"
	// command     *exec.Cmd
}

// NewSCP instantiates a new instance of SCP
func NewSCP(obj string, destination string) *SCP {
	return &SCP{Object: obj, Destination: destination}
}

// ProcessData sends all data to outputChan
func (s *SCP) ProcessData(d etldata.Payload, outputChan chan etldata.Payload, killChan chan error) {
	outputChan <- d
}

// Finish defers to Run
func (s *SCP) Finish(outputChan chan etldata.Payload, killChan chan error) {
	s.Run(killChan)
}

// Run executes the scp command from the attributes of the SCP struct
func (s *SCP) Run(killChan chan error) {
	scpParams := []string{}
	if s.Port != "" {
		scpParams = append(scpParams, fmt.Sprintf("-P %v", s.Port))
	}
	scpParams = append(scpParams, s.Object)
	scpParams = append(scpParams, s.Destination)

	cmd := exec.Command("scp", scpParams...)
	_, err := cmd.Output()
	etlutil.KillPipelineIfErr(err, killChan)
}
