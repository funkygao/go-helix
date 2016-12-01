package zk

import (
	"strings"
)

func parseZkConnStr(connStr string) (servers []string, chroot string, err error) {
	offset := strings.Index(connStr, "/")
	if offset == -1 {
		// no chroot
		servers = strings.Split(connStr, ",")
		return
	}

	chrootPath := connStr[offset:len(connStr)]
	if len(chrootPath) > 1 {
		chroot = strings.TrimRight(chrootPath, "/")
		// validate path TODO
	}
	connStr = connStr[:offset]
	servers = strings.Split(connStr, ",")

	return
}
