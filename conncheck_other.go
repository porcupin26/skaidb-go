//go:build !(linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos)

package skaidb

import "net"

// peerClosed reports nothing where the driver has no way to look at a socket
// without waiting on it: a connection the server closed while it sat idle is
// found by the statement's own write or read there.
func peerClosed(net.Conn) error { return nil }
