package pipeline

type Stage interface {

	// Name returns name of the stage.
	Name() string

	Init()

	PreProcess()

	Process(evt string) error

	PostProcess()

	Release()
}
