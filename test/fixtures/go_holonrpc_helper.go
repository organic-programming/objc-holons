package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"nhooyr.io/websocket"
)

type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      interface{}     `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

func main() {
	mode := "echo"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	var heartbeatCount int64
	var dropped int32

	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			Subprotocols:       []string{"holon-rpc"},
			InsecureSkipVerify: true,
		})
		if err != nil {
			http.Error(w, "upgrade failed", http.StatusBadRequest)
			return
		}
		defer c.CloseNow()

		ctx := r.Context()
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}

			var msg rpcMessage
			if err := json.Unmarshal(data, &msg); err != nil {
				_ = writeError(ctx, c, nil, -32700, "parse error")
				continue
			}
			if msg.JSONRPC != "2.0" {
				_ = writeError(ctx, c, msg.ID, -32600, "invalid request")
				continue
			}
			if msg.Method == "" {
				continue
			}

			switch msg.Method {
			case "rpc.heartbeat":
				atomic.AddInt64(&heartbeatCount, 1)
				_ = writeResult(ctx, c, msg.ID, map[string]interface{}{})
			case "echo.v1.Echo/Ping":
				var params map[string]interface{}
				_ = json.Unmarshal(msg.Params, &params)
				if params == nil {
					params = map[string]interface{}{}
				}
				_ = writeResult(ctx, c, msg.ID, params)
				if mode == "drop-once" && atomic.CompareAndSwapInt32(&dropped, 0, 1) {
					time.Sleep(100 * time.Millisecond)
					_ = c.Close(websocket.StatusNormalClosure, "drop once")
					return
				}
			case "echo.v1.Echo/HeartbeatCount":
				_ = writeResult(ctx, c, msg.ID, map[string]interface{}{"count": atomic.LoadInt64(&heartbeatCount)})
			case "echo.v1.Echo/CallClient":
				callID := "s1"
				if err := writeRequest(ctx, c, callID, "client.v1.Client/Hello", map[string]interface{}{"name": "go"}); err != nil {
					_ = writeError(ctx, c, msg.ID, 13, err.Error())
					continue
				}

				innerResult, callErr := waitForResponse(ctx, c, callID)
				if callErr != nil {
					_ = writeError(ctx, c, msg.ID, 13, callErr.Error())
					continue
				}
				_ = writeResult(ctx, c, msg.ID, innerResult)
			default:
				_ = writeError(ctx, c, msg.ID, -32601, fmt.Sprintf("method %q not found", msg.Method))
			}
		}
	})

	srv := &http.Server{Handler: h}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}
	}()

	fmt.Printf("ws://%s/rpc\n", ln.Addr().String())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}

func writeRequest(ctx context.Context, c *websocket.Conn, id interface{}, method string, params map[string]interface{}) error {
	payload, err := json.Marshal(rpcMessage{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  mustRaw(params),
	})
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, payload)
}

func writeResult(ctx context.Context, c *websocket.Conn, id interface{}, result interface{}) error {
	payload, err := json.Marshal(rpcMessage{
		JSONRPC: "2.0",
		ID:      id,
		Result:  mustRaw(result),
	})
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, payload)
}

func writeError(ctx context.Context, c *websocket.Conn, id interface{}, code int, message string) error {
	payload, err := json.Marshal(rpcMessage{
		JSONRPC: "2.0",
		ID:      id,
		Error: &rpcError{
			Code:    code,
			Message: message,
		},
	})
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, payload)
}

func waitForResponse(ctx context.Context, c *websocket.Conn, expectedID string) (map[string]interface{}, error) {
	deadlineCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for {
		_, data, err := c.Read(deadlineCtx)
		if err != nil {
			return nil, err
		}

		var msg rpcMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		id, _ := msg.ID.(string)
		if id != expectedID {
			continue
		}
		if msg.Error != nil {
			return nil, fmt.Errorf("client error: %d %s", msg.Error.Code, msg.Error.Message)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(msg.Result, &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

func mustRaw(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return json.RawMessage(b)
}
