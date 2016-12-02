package zk

import (
	"testing"

	"github.com/funkygao/assert"
)

func TestZkParseConnStr(t *testing.T) {
	fixtures := assert.Fixtures{
		assert.Fixture{
			Expected: "",
			Input:    "127.0.0.1:2181,127.1.1.1:2191",
		},

		assert.Fixture{
			Expected: "/abc",
			Input:    "127.0.0.1:2181/abc/",
		},

		assert.Fixture{
			Expected: "/abc",
			Input:    "127.0.0.1:2181/abc",
		},
	}

	for _, f := range fixtures {
		servers, chroot, err := parseZkConnStr(f.Input.(string))
		assert.Equal(t, f.Expected.(string), chroot)
		assert.Equal(t, nil, err)
		assert.Equal(t, true, len(servers) > 0)
	}

}
