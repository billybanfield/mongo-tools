package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/spacemonkeygo/openssl"
)

func main() {
	// parse flags
	//make an openssl context
	// use that context to dial the server 100 times
	caFile := flag.String("sslCAFile", "", "")
	pemFile := flag.String("sslPEMKeyFile", "", "")
	addr := flag.String("host", "", "")
	serial := flag.Bool("serial", false, "")
	numConn := flag.Int("numConn", 100, "")

	flag.Parse()

	if *caFile == "" || *pemFile == "" {
		panic("must specify both cafile and pemfile")
	}
	ctx, err := setupCtx(*caFile, *pemFile)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	for i := 0; i < *numConn; i++ {
		d := func() {
			fmt.Println("opening connection")
			var flags openssl.DialFlags
			flags = 0
			conn, err := openssl.Dial("tcp", *addr, ctx, flags)
			if err != nil {
				fmt.Println("dial error: ", err)
			}
			fmt.Println("opened conn: ", conn.RemoteAddr())
			time.Sleep(1)
			conn.Close()
			fmt.Println("closed conn: ", conn.RemoteAddr())
			wg.Done()
		}
		if !*serial {
			wg.Add(1)
			go d()
		} else {
			wg.Add(1)
			d()
		}
	}
	wg.Wait()
	fmt.Println("done")
}

// Creates and configures an openssl.Ctx
func setupCtx(caFile, pemKeyFile string) (*openssl.Ctx, error) {
	var ctx *openssl.Ctx
	var err error

	if ctx, err = openssl.NewCtxWithVersion(openssl.AnyVersion); err != nil {
		return nil, fmt.Errorf("failure creating new openssl context with "+
			"NewCtxWithVersion(AnyVersion): %v", err)
	}

	// HIGH - Enable strong ciphers
	// !EXPORT - Disable export ciphers (40/56 bit)
	// !aNULL - Disable anonymous auth ciphers
	// @STRENGTH - Sort ciphers based on strength
	ctx.SetCipherList("HIGH:!EXPORT:!aNULL@STRENGTH")

	// add the PEM key file with the cert and private key, if specified
	if err = ctx.UseCertificateChainFile(pemKeyFile); err != nil {
		return nil, fmt.Errorf("UseCertificateChainFile: %v", err)
	}

	if err = ctx.UsePrivateKeyFile(pemKeyFile, openssl.FiletypePEM); err != nil {
		return nil, fmt.Errorf("UsePrivateKeyFile: %v", err)
	}

	// Verify that the certificate and the key go together.
	if err = ctx.CheckPrivateKey(); err != nil {
		return nil, fmt.Errorf("CheckPrivateKey: %v", err)
	}

	// If renegotiation is needed, don't return from recv() or send() until it's successful.
	// Note: this is for blocking sockets only.
	ctx.SetMode(openssl.AutoRetry)

	// Disable session caching (see SERVER-10261)
	ctx.SetSessionCacheMode(openssl.SessionCacheOff)

	calist, err := openssl.LoadClientCAFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("LoadClientCAFile: %v", err)
	}

	ctx.SetClientCAList(calist)
	if err = ctx.LoadVerifyLocations(caFile, ""); err != nil {
		return nil, fmt.Errorf("LoadVerifyLocations: %v", err)
	}
	ctx.SetVerify(openssl.VerifyPeer, nil)

	return ctx, nil
}
