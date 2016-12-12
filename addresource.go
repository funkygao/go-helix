package helix

type AddResourceOption struct {
	Partitions               int
	StateModel               string
	RebalancerMode           string
	RebalanceStrategy        string
	BucketSize               int
	MaxPartitionsPerInstance int
}

func DefaultAddResourceOption(partitions int, stateModel string) AddResourceOption {
	return AddResourceOption{
		Partitions:        partitions,
		StateModel:        stateModel,
		RebalancerMode:    RebalancerModeSemiAuto,
		RebalanceStrategy: "DEFAULT",
	}
}

func (opt AddResourceOption) Valid() bool {
	if opt.Partitions < 1 || opt.StateModel == "" || opt.RebalancerMode == "" {
		return false
	}
	return true
}
