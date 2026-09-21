package faas

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"strconv"
)

// LocalCommand describes a local executable used to simulate an isolated
// function runtime. The kernel launches it per invocation and hard-kills the
// OS process when the invocation deadline expires.
type LocalCommand struct {
	Path string
	Args []string
	Env  []string
}

type runtimeEnvelope struct {
	RequestID string `json:"request_id"`
	Attempt   int    `json:"attempt"`
	Key       string `json:"idempotency_key"`
	Payload   []byte `json:"payload"`
}

// Runtime executes one attempt. Implementations must return promptly after
// ctx cancellation and must not mutate InvocationContext.Payload.
type Runtime interface {
	Run(ctx context.Context, call *InvocationContext, key string) ([]byte, error)
}

type inProcessRuntime struct {
	handler HandlerFunc
}

func (r inProcessRuntime) Run(ctx context.Context, call *InvocationContext, key string) ([]byte, error) {
	payload, err := r.handler(ctx, call)
	if err != nil {
		var pe *Error
		if !errors.As(err, &pe) {
			return nil, functionError("handler returned error", err)
		}
	}
	return payload, err
}

type localProcessRuntime struct {
	command LocalCommand
}

func (r localProcessRuntime) Run(ctx context.Context, call *InvocationContext, key string) ([]byte, error) {
	input := runtimeEnvelope{RequestID: call.RequestID, Attempt: call.Attempt, Key: key, Payload: append([]byte(nil), call.Payload...)}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, platformError("encode local runtime input: %v", err)
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, r.command.Path, r.command.Args...)
	cmd.Stdin = bytes.NewReader(encoded)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = r.command.Env
	cmd.Cancel = func() error { return cmd.Process.Kill() }

	runErr := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return nil, timeoutError("local process runtime killed after deadline")
	}
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			detail := stderr.String()
			if detail == "" {
				detail = exitErr.String()
			}
			return nil, functionError("local function exited with status "+strconv.Itoa(exitErr.ExitCode()), errors.New(detail))
		}
		return nil, platformError("start local function process: %v", runErr)
	}
	return stdout.Bytes(), nil
}

func runtimeForSpec(spec FunctionSpec) (Runtime, error) {
	if spec.Handler != nil && spec.Command != nil {
		return nil, invalidArgument("function %q has two runtimes", spec.Name)
	}
	if spec.Handler != nil {
		return inProcessRuntime{handler: spec.Handler}, nil
	}
	if spec.Command != nil {
		if spec.Command.Path == "" {
			return nil, invalidArgument("function %q local command path is empty", spec.Name)
		}
		command := *spec.Command
		if command.Args != nil {
			command.Args = append([]string(nil), command.Args...)
		}
		if command.Env != nil {
			command.Env = append([]string(nil), command.Env...)
		}
		return localProcessRuntime{command: command}, nil
	}
	return nil, invalidArgument("function %q has no runtime", spec.Name)
}
