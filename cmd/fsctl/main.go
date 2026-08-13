// Command fsctl is a small CLI client for the fsbridge control endpoint.
//
//	fsctl originate loopback/9999 park
//	fsctl hangup <uuid>
//	fsctl transfer <uuid> 1002
//	fsctl hold <uuid> | unhold <uuid>
//	fsctl dtmf <uuid> 1234
//	fsctl clear <uuid>
//	fsctl events            # just stream events
//
// Replies and events are printed as JSON, one per line.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/coder/websocket"
)

func main() {
	addr := flag.String("addr", "ws://localhost:8090/control", "control endpoint URL")
	follow := flag.Duration("follow", 10*time.Second, "how long to keep streaming events after the command")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: fsctl [flags] <command> [args...]")
		os.Exit(2)
	}

	var cmd *ControlCommand
	if args[0] == "events" {
		cmd = nil
	} else {
		cmd = buildCommand(args)
		if cmd == nil {
			fmt.Fprintf(os.Stderr, "unknown/invalid command: %v\n", args)
			os.Exit(2)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), *follow+10*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, *addr, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if cmd != nil {
		data, _ := json.Marshal(cmd)
		if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
	}

	deadline := time.After(*follow)
	gotReply := cmd == nil
	for {
		if gotReply && cmd != nil {
			// keep reading events until follow expires
		}
		select {
		case <-deadline:
			return
		default:
		}
		rctx, rcancel := context.WithTimeout(ctx, *follow)
		_, data, err := conn.Read(rctx)
		rcancel()
		if err != nil {
			return
		}
		fmt.Println(string(data))
		gotReply = true
	}
}

func buildCommand(args []string) *ControlCommand {
	id := fmt.Sprintf("fsctl-%d", time.Now().UnixNano())
	mk := func(cmd string, argPairs ...string) *ControlCommand {
		m := map[string]string{}
		for i := 0; i+1 < len(argPairs); i += 2 {
			m[argPairs[i]] = argPairs[i+1]
		}
		raw, _ := json.Marshal(m)
		return &ControlCommand{ID: id, Cmd: cmd, Args: raw}
	}

	switch args[0] {
	case "events":
		return nil
	case "originate":
		if len(args) < 2 {
			return nil
		}
		pairs := []string{"dest", args[1]}
		if len(args) > 2 && args[2] != "" {
			pairs = append(pairs, "app", args[2])
		}
		return mk("originate", pairs...)
	case "hangup":
		if len(args) < 2 {
			return nil
		}
		return mk("hangup", "uuid", args[1])
	case "transfer":
		if len(args) < 3 {
			return nil
		}
		return mk("transfer", "uuid", args[1], "dest", args[2])
	case "hold":
		if len(args) < 2 {
			return nil
		}
		return mk("hold", "uuid", args[1])
	case "unhold":
		if len(args) < 2 {
			return nil
		}
		return mk("unhold", "uuid", args[1])
	case "dtmf":
		if len(args) < 3 {
			return nil
		}
		return mk("dtmf", "uuid", args[1], "digits", args[2])
	case "clear":
		if len(args) < 2 {
			return nil
		}
		return mk("clear_playback", "uuid", args[1])
	}
	return nil
}

// ControlCommand mirrors bridge.ControlCommand (kept local so fsctl has no
// import coupling to the server internals).
type ControlCommand struct {
	ID   string          `json:"id"`
	Cmd  string          `json:"cmd"`
	Args json.RawMessage `json:"args"`
}
