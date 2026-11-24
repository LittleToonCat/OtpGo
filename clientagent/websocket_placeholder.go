//go:build !no_clientagent && no_websocket

package clientagent

func (c *ClientAgent) startWebsocket(bind string) {
	c.log.Fatal("Websocket support was omitted from this build (no_websocket).")
}
