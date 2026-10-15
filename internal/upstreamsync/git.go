package upstreamsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
)

// CommandResult keeps stdout and stderr separate so callers can parse machine
// output without losing diagnostics.
type CommandResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// CommandRunner is the process boundary used by the synchronizer and tests.
type CommandRunner interface {
	Run(ctx context.Context, dir, name string, args ...string) (CommandResult, error)
}

type execCommandRunner struct{}

func (execCommandRunner) Run(ctx context.Context, dir, name string, args ...string) (CommandResult, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	result := CommandResult{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, &CommandError{
			Name:     name,
			Args:     append([]string(nil), args...),
			Dir:      dir,
			ExitCode: result.ExitCode,
			Output:   strings.TrimSpace(result.Stderr + result.Stdout),
			Cause:    err,
		}
	}
	return result, fmt.Errorf("start %s in %s: %w", name, dir, err)
}

// CommandError represents a command that started but exited unsuccessfully.
type CommandError struct {
	Name     string
	Args     []string
	Dir      string
	ExitCode int
	Output   string
	Cause    error
}

func (e *CommandError) Error() string {
	if e.Output == "" {
		return fmt.Sprintf("%s %s in %s exited %d: %v", e.Name, strings.Join(e.Args, " "), e.Dir, e.ExitCode, e.Cause)
	}
	return fmt.Sprintf("%s %s in %s exited %d: %s", e.Name, strings.Join(e.Args, " "), e.Dir, e.ExitCode, e.Output)
}

func (e *CommandError) Unwrap() error { return e.Cause }

func runChecked(ctx context.Context, runner CommandRunner, dir, name string, args ...string) (CommandResult, error) {
	result, err := runner.Run(ctx, dir, name, args...)
	if err != nil {
		return result, err
	}
	if result.ExitCode != 0 {
		return result, &CommandError{Name: name, Args: args, Dir: dir, ExitCode: result.ExitCode, Output: strings.TrimSpace(result.Stderr + result.Stdout)}
	}
	return result, nil
}

func isExitCode(err error, code int) bool {
	var commandErr *CommandError
	return errors.As(err, &commandErr) && commandErr.ExitCode == code
}
