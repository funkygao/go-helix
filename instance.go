package helix

type InstanceConfig struct {
	Host, Port string
}

func (ic InstanceConfig) Node() string {
	return ic.Host + "_" + ic.Port
}

func (ic InstanceConfig) Validate() error {
	// TODO
	return nil
}
