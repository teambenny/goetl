package goetl

// PipelineStage holds one or more Processor instances.
type PipelineStage struct {
	processors []*DataProcessor
}

// NewPipelineStage creates a PipelineStage instance given a series
// of Processors. DataProcessor is a wrapper around an object implementing
// the Processor interface. The syntax used to create PipelineLayouts
// abstracts this type away from your implementing code. For example:
//
//     layout, err := goetl.NewPipelineLayout(
//             goetl.NewPipelineStage(
//                      goetl.Do(aProcessor).Outputs(anotherProcessor),
//                      // ...
//             ),
//             // ...
//     )
//
// Notice how the goetl.Do() and Outputs() functions allow you to insert
// Processor instances into your PipelineStages without having to
// worry about the internal DataProcessor type or how any of the
// channel management works behind the scenes.
//
// See the goetl package documentation for more code examples.
func NewPipelineStage(processors ...*DataProcessor) *PipelineStage {
	return &PipelineStage{processors}
}

func (s *PipelineStage) hasProcessor(p Processor) bool {
	for i := range s.processors {
		if s.processors[i].Processor == p {
			return true
		}
	}
	return false
}

func (s *PipelineStage) hasOutput(p Processor) bool {
	for i := range s.processors {
		for j := range s.processors[i].outputs {
			if s.processors[i].outputs[j] == p {
				return true
			}
		}
	}
	return false
}
