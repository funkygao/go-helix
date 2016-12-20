package model

import (
	"fmt"
)

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

func (ic InstanceConfig) InstanceName() string {
	return ic.Node()
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

func (ic *InstanceConfig) RemoveTag(tag string) {
	ic.RemoveListField("TAG_LIST", tag)
}

func (ic *InstanceConfig) ContainsTag(tag string) bool {
	for _, t := range ic.Tags() {
		if t == tag {
			return true
		}
	}
	return false
}

func (ic InstanceConfig) DisabledPartitions() []string {
	return ic.GetListField("HELIX_DISABLED_PARTITION")
}

func (ic InstanceConfig) String() string {
	return fmt.Sprintf("%s enabled:%v tags:%+v", ic.Node(), ic.IsEnabled(), ic.Tags())
}

// TODO
func (ic InstanceConfig) EnablePartition(partition string, yes bool) {

}

// TODO
func (ic InstanceConfig) Validate() error {
	return nil
}
