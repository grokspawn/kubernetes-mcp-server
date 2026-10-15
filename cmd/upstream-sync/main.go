package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/containers/kubernetes-mcp-server/internal/upstreamsync"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "upstream-sync:", err)
		os.Exit(1)
	}
}

func run() error {
	var cfg upstreamsync.Config
	var mode string
	flag.StringVar(&mode, "mode", "plan", "Operation mode: plan or publish")
	flag.StringVar(&cfg.RepoDir, "repo-dir", ".", "Path to the shared upstream/downstream clone")
	flag.StringVar(&cfg.UpstreamRemote, "upstream-remote", "upstream", "Git remote for upstream")
	flag.StringVar(&cfg.UpstreamRepo, "upstream-repo", "containers/kubernetes-mcp-server", "Expected GitHub upstream repository")
	flag.StringVar(&cfg.DownstreamRemote, "downstream-remote", "origin", "Git remote for downstream")
	flag.StringVar(&cfg.DownstreamRepo, "downstream-repo", "", "Expected GitHub downstream repository")
	flag.StringVar(&cfg.OwnerRepo, "owner-repo", "", "GitHub repository where the sync PR is created")
	flag.StringVar(&cfg.SyncBranch, "sync-branch", "upstream-sync", "Persistent sync branch and PR head")
	flag.Parse()
	if mode != "plan" && mode != "publish" {
		return fmt.Errorf("unknown mode %q (expected plan or publish)", mode)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	syncer := upstreamsync.New()
	plan, err := syncer.BuildPlan(ctx, cfg)
	if err != nil {
		return err
	}
	if mode == "plan" {
		return printJSON(plan)
	}
	report, err := syncer.Publish(ctx, cfg, plan)
	if err != nil {
		_ = printJSON(report)
		return err
	}
	return printJSON(report)
}

func printJSON(value any) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	return nil
}
