package pipeline

type Pipeline struct {
	stages []Stage
}

func NewPipeline() *Pipeline {
	return &Pipeline{
		stages: []Stage{},
	}
}

func (p *Pipeline) AddStage(s Stage) {
	p.stages = append(p.stages, s)
	s.Init()
}

func (p *Pipeline) HandleEvent(evt string) (err error) {
	for _, s := range p.stages {
		s.PreProcess()
		if err = s.Process(evt); err != nil {
			return
		}
		s.PostProcess()
	}

	return
}

func (p *Pipeline) Finish() {

}
