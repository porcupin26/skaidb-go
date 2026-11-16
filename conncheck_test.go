//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package skaidb

// canLook: the driver looks at an idle socket on this platform (peerClosed).
const canLook = true
