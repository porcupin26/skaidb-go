//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package skaidb

import (
	"errors"
	"io"
	"net"
	"syscall"
)

// errStrayBytes: bytes arrived on a connection that was waiting for nothing.
var errStrayBytes = errors.New("unexpected bytes from the server")

// peerClosed looks at an idle socket without waiting: one read on its
// non-blocking descriptor, which costs a system call and no round trip.
//
// Between two requests the server sends nothing, so a socket with nothing to
// read (EAGAIN) is in order and anything else means the connection is no
// longer usable: end-of-file (the server closed it — a restart, a failover,
// an idle timeout), an error (a reset), or bytes that answer no request,
// which leave the connection out of step whatever they are. The error says
// which; nil is "nothing to see", and that is also the answer for a
// connection that is not a socket and cannot be looked at.
func peerClosed(nc net.Conn) error {
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return nil
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var seen error
	err = rc.Read(func(fd uintptr) bool {
		var one [1]byte
		for {
			n, err := syscall.Read(int(fd), one[:])
			switch {
			case err == syscall.EINTR:
				continue
			case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
			case err != nil:
				seen = err
			case n == 0:
				seen = io.EOF
			default:
				seen = errStrayBytes
			}
			return true // never wait for the socket to become readable
		}
	})
	if err != nil {
		return err
	}
	return seen
}
