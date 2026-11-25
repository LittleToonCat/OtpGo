//go:build !no_clientagent && !no_websocket

package clientagent

import (
	"context"
	"errors"
	gonet "net"
	"net/http"
	"net/netip"
	"otpgo/core"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type websocketConn struct {
	gonet.Conn
	remote gonet.Addr
	local  gonet.Addr
}

func (w *websocketConn) RemoteAddr() gonet.Addr {
	return w.remote
}

func (w *websocketConn) LocalAddr() gonet.Addr {
	return w.local
}

func tcpAddrFromString(addr string) *gonet.TCPAddr {
	if resolved, err := gonet.ResolveTCPAddr("tcp", addr); err == nil {
		return resolved
	}
	return &gonet.TCPAddr{IP: gonet.IPv4zero}
}

var forwardedHeaders = []string{"CF-Connecting-IP", "X-Real-IP", "X-Forwarded-For", "X-Forwarded-Host"}

func forwardedAddr(r *http.Request) *gonet.TCPAddr {
	candidates := make([]string, 0, len(forwardedHeaders))
	for _, header := range forwardedHeaders {
		candidates = append(candidates, strings.TrimSpace(strings.Split(r.Header.Get(header), ",")[0]))
	}
	for _, value := range candidates {
		if value == "" {
			continue
		}
		if ap, err := netip.ParseAddrPort(value); err == nil {
			return gonet.TCPAddrFromAddrPort(ap)
		}
		host := strings.TrimSuffix(strings.TrimPrefix(value, "["), "]")
		if addr, err := netip.ParseAddr(host); err == nil {
			return gonet.TCPAddrFromAddrPort(netip.AddrPortFrom(addr, 0))
		}
	}
	return tcpAddrFromString(r.RemoteAddr)
}

func (c *ClientAgent) startWebsocket(bind string) {
	listener, err := gonet.Listen("tcp", bind)
	if err != nil {
		c.log.Fatal(err.Error())
	}

	server := &http.Server{
		Handler:           http.HandlerFunc(c.handleWebsocket),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-core.StopChan
		server.Close()
	}()

	c.log.Infof("Opened websocket listening socket at %s", bind)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.log.Fatal(err.Error())
		}
	}()
}

func (c *ClientAgent) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		c.log.Debugf("Websocket upgrade from %s failed: %s", r.RemoteAddr, err.Error())
		return
	}

	remote := tcpAddrFromString(r.RemoteAddr)
	if c.config.Websocket_Proxy {
		remote = forwardedAddr(r)
	}

	local := tcpAddrFromString(r.Context().Value(http.LocalAddrContextKey).(gonet.Addr).String())
	conn := &websocketConn{
		Conn:   websocket.NetConn(context.Background(), ws, websocket.MessageBinary),
		remote: remote,
		local:  local,
	}
	c.HandleConnect(conn)
}
