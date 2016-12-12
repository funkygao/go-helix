package model

type InstanceConfig struct {
	*Record
}

func NewInstanceConfigFromRecord(record *Record) *InstanceConfig {
	return &InstanceConfig{
		Record: record,
	}
}

func (ic InstanceConfig) Host() string {
	return ic.GetStringField("HELIX_HOST", "")
}

func (ic InstanceConfig) SetHost(host string) {
	ic.SetStringField("HELIX_HOST", host)
}

func (ic InstanceConfig) Port() string {
	return ic.GetStringField("HELIX_PORT", "")
}

func (ic InstanceConfig) SetPort(port string) {
	ic.SetStringField("HELIX_PORT", port)
}

func (ic *InstanceConfig) Enable(enabled bool) {
	ic.SetBooleanField("HELIX_ENABLED", enabled)
}

func (ic *InstanceConfig) IsEnabled() bool {
	return ic.GetBooleanField("HELIX_ENABLED", true)
}

func (ic InstanceConfig) Node() string {
	return ic.Host() + "_" + ic.Port()
}

func (ic InstanceConfig) Tags() []string {
	return ic.GetListField("TAG_LIST")
}

func (ic *InstanceConfig) AddTag(tag string) {
	ic.AddListField("TAG_LIST", tag)
}

func (ic InstanceConfig) DisabledPartitions() []string {
	return ic.GetListField("HELIX_DISABLED_PARTITION")
}

// TODO
func (ic InstanceConfig) EnablePartition(partition string, yes bool) {

}

// TODO
func (ic InstanceConfig) Validate() error {
	return nil
}
