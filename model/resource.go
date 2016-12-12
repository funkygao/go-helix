package model

// A resource contains a set of partitions and its replicas are managed by a state model.
type Resource struct {
	name         string
	partitionMap map[string]struct{}

	stateModelDef     string
	stateModelFactory string

	bucketSize       int
	batchMessageMode bool

	group string
	tag   string
}

func NewResource(name string) *Resource {
	return &Resource{
		name:         name,
		partitionMap: make(map[string]struct{}),
	}
}

func (r Resource) Name() string {
	return r.name
}

func (r Resource) BucketSize() int {
	return r.bucketSize
}

func (r *Resource) SetBucketSize(size int) {
	r.bucketSize = size
}

func (r Resource) BatchMessageMode() bool {
	return r.batchMessageMode
}

func (r *Resource) SetBatchMessageMode(yes bool) {
	r.batchMessageMode = yes
}

func (r *Resource) SetStateModelDef(def string) {
	r.stateModelDef = def
}

func (r *Resource) SetStateModelFactory(f string) {
	if f == "" {
		f = "DEFAULT"
	}
	r.stateModelFactory = f
}

func (r Resource) Partitions() []string {
	rs := make([]string, 0, len(r.partitionMap))
	for p := range r.partitionMap {
		rs = append(rs, p)
	}
	return rs
}

func (r *Resource) AddPartition(partitionName string) *Resource {
	r.partitionMap[partitionName] = struct{}{}
	return r
}

func (r Resource) String() string {
	return ""
}
