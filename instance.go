package helix

type InstanceConfig struct {
	Host, Port string
}

func (ic *InstanceConfig) SetHost(host string) {
	ic.Host = host
}

func (ic *InstanceConfig) SetPort(port string) {
	ic.Port = port
}

func (ic InstanceConfig) Node() string {
	return ic.Host + "_" + ic.Port
}
