//go:build !(linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos)

package skaidb

// canLook: the driver cannot look at an idle socket on this platform, so the
// tests of what the look buys do not run here.
const canLook = false
