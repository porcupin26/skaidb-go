package skaidb

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// EXTERNAL (PROTOCOL.md §2.4): AuthStart carries the username, an EMPTY
// client nonce and mechanism byte 2; the server answers AuthOutcome at once
// and the zero signature of an Ok is accepted without verification.
func TestExternalAuthFrameAndOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, user string
		outcome    []byte
		wantErr    string
	}{
		{"ok with a username", "ada", append([]byte{13, 1}, make([]byte, 32)...), ""},
		{"ok taking the CN", "", append([]byte{13, 1}, make([]byte, 32)...), ""},
		{"denied", "ada", appendStr([]byte{13, 0}, "certificate CN does not match"), "certificate CN does not match"},
		{"truncated ok", "ada", []byte{13, 1, 0, 0}, "handshake decode"},
		{"not an outcome", "ada", []byte{11}, "bad handshake outcome"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan []byte, 1)
			c := fakeServer(t, func(s net.Conn) {
				req, err := readReq(s)
				if err != nil {
					return
				}
				got <- req
				s.Write(serverFrame(tc.outcome))
			})
			err := c.externalAuth(tc.user)
			want := appendStr(appendStr([]byte{10}, tc.user), "")
			want = append(want, 2)
			if req := <-got; !bytes.Equal(req, want) {
				t.Errorf("AuthStart = %x, want %x", req, want)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// Certificate login refuses to run over plaintext: without TLS there is no
// certificate for the server to authenticate.
func TestCertificateLoginNeedsTLS(t *testing.T) {
	c := fakeServer(t, func(s net.Conn) { _, _ = readReq(s) })
	if err := c.handshakeCertificate("ada"); err == nil || !strings.Contains(err.Error(), "needs TLS") {
		t.Fatalf("err = %v, want a needs-TLS error", err)
	}
}

func TestParseDSNCertificateOptions(t *testing.T) {
	cfg, err := parseDSN("skaidb://h/?auth_mechanism=certificate&tls_client_cert=c.pem&tls_client_key=k.pem&tls_ca=ca.pem")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.certAuth || !cfg.tls || cfg.user != "" || cfg.tlsClientCert != "c.pem" || cfg.tlsClientKey != "k.pem" {
		t.Errorf("cfg = %+v: want certificate auth over TLS with no username", cfg)
	}
	cfg, err = parseDSN("skaidb://ada@h/?auth_mechanism=CERTIFICATE&tls_client_cert=c.pem&tls_client_key=k.pem")
	if err != nil || cfg.user != "ada" || !cfg.certAuth {
		t.Errorf("cfg = %+v, err = %v: want the DSN username kept", cfg, err)
	}
	// A client certificate alone turns TLS on but keeps SCRAM.
	cfg, err = parseDSN("skaidb://u:p@h/?tls_client_cert=c.pem&tls_client_key=k.pem")
	if err != nil || cfg.certAuth || !cfg.tls || cfg.user != "u" {
		t.Errorf("cfg = %+v, err = %v", cfg, err)
	}
	for _, bad := range []string{
		"skaidb://h/?auth_mechanism=certificate",
		"skaidb://h/?tls_client_cert=c.pem",
		"skaidb://h/?tls_client_key=k.pem",
		"skaidb://h/?auth_mechanism=kerberos",
	} {
		if _, err := parseDSN(bad); err == nil {
			t.Errorf("parseDSN(%q) accepted", bad)
		}
	}
}

// ---- end to end over real TLS ---------------------------------------------

type testPKI struct {
	dir                           string
	caPool                        *x509.CertPool
	server                        tls.Certificate
	caFile, clientCert, clientKey string
}

func newTestPKI(t *testing.T, clientCN string) *testPKI {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	leaf := func(serial int64, cn string, usage x509.ExtKeyUsage, dns []string) ([]byte, *ecdsa.PrivateKey) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
			DNSNames: dns,
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der, key
	}
	write := func(name, typ string, der []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	pki := &testPKI{dir: dir, caPool: x509.NewCertPool()}
	pki.caPool.AddCert(ca)
	pki.caFile = write("ca.pem", "CERTIFICATE", caDER)
	srvDER, srvKey := leaf(2, "skaidb", x509.ExtKeyUsageServerAuth, []string{"skaidb"})
	pki.server = tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}
	cliDER, cliKey := leaf(3, clientCN, x509.ExtKeyUsageClientAuth, nil)
	keyDER, err := x509.MarshalECPrivateKey(cliKey)
	if err != nil {
		t.Fatal(err)
	}
	pki.clientCert = write("client.pem", "CERTIFICATE", cliDER)
	pki.clientKey = write("client.key", "EC PRIVATE KEY", keyDER)
	return pki
}

// The whole path through sql.Open: the DSN's client certificate is presented
// in the TLS handshake (the server requires and verifies it), AuthStart is
// the EXTERNAL frame, and the connection then serves statements.
func TestCertificateLoginEndToEnd(t *testing.T) {
	pki := newTestPKI(t, "ada")
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{pki.server},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pki.caPool,
	})
	if err != nil {
		t.Skipf("no loopback listener available: %v", err)
	}
	defer ln.Close()
	seen := make(chan string, 8)
	go func() {
		s, err := ln.Accept()
		if err != nil {
			return
		}
		defer s.Close()
		tc := s.(*tls.Conn)
		if err := tc.Handshake(); err != nil {
			seen <- "tls: " + err.Error()
			return
		}
		seen <- "cn=" + tc.ConnectionState().PeerCertificates[0].Subject.CommonName
		req, err := readReq(s)
		if err != nil {
			return
		}
		want := append(appendStr(appendStr([]byte{10}, ""), ""), 2)
		if !bytes.Equal(req, want) {
			seen <- "bad start"
			return
		}
		seen <- "external"
		s.Write(serverFrame(append([]byte{13, 1}, make([]byte, 32)...)))
		for {
			req, err := readReq(s)
			if err != nil {
				return
			}
			switch req[0] {
			case 8:
				s.Write(ddlFrame())
			case 1:
				seen <- "sql " + string(req[6:])
				s.Write(mutationFrame(1))
			}
		}
	}()

	dsn := "skaidb://" + ln.Addr().String() + "/?auth_mechanism=certificate" +
		"&tls_ca=" + pki.caFile + "&tls_client_cert=" + pki.clientCert + "&tls_client_key=" + pki.clientKey
	db, err := sql.Open("skaidb", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	res, err := db.Exec("DELETE FROM t")
	if err != nil {
		t.Fatalf("Exec over a certificate login: %v", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		t.Errorf("affected = %d", n)
	}
	for _, w := range []string{"cn=ada", "external", "sql DELETE FROM t"} {
		select {
		case got := <-seen:
			if got != w {
				t.Fatalf("server saw %q, want %q", got, w)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("server never saw %q", w)
		}
	}
}

// An unreadable key pair is a connect error that names the options.
func TestCertificateLoginReportsABadKeyPair(t *testing.T) {
	pki := newTestPKI(t, "ada")
	cfg, err := parseDSN("skaidb://127.0.0.1:1/?auth_mechanism=certificate&tls_client_cert=" +
		pki.clientCert + "&tls_client_key=" + pki.caFile)
	if err != nil {
		t.Fatal(err)
	}
	client, server := net.Pipe()
	defer server.Close()
	if _, err := tlsWrap(client, cfg); err == nil || !strings.Contains(err.Error(), "tls_client_cert") {
		t.Fatalf("err = %v, want a key-pair error", err)
	}
}

// A SCRAM Ok outcome cut short of its 32-byte signature is a decode error,
// even for an empty password (which skips the signature check).
func TestScramTruncatedOutcomeIsADecodeError(t *testing.T) {
	c := fakeServer(t, func(s net.Conn) {
		req, err := readReq(s)
		if err != nil {
			return
		}
		r := &reader{buf: req[1:]}
		_ = r.text()
		nonce := r.text()
		ch := appendStr([]byte{11}, "salt")
		ch = append(ch, 1, 0, 0, 0)
		ch = appendStr(ch, nonce+"s")
		s.Write(serverFrame(ch))
		if _, err := readReq(s); err != nil {
			return
		}
		s.Write(serverFrame([]byte{13, 1, 0, 0}))
	})
	if err := c.handshake("anonymous", ""); err == nil || !strings.Contains(err.Error(), "handshake decode") {
		t.Fatalf("err = %v, want a handshake decode error", err)
	}
}
