package upstreamsync

import (
	"context"
	"fmt"
	"strings"
)

// MergeabilityContext is the commit status required by downstream branch protection.
const MergeabilityContext = "upstream-sync/mergeable"

// Publisher creates or updates the single sync PR and its required status.
type Publisher interface {
	UpsertPR(ctx context.Context, cfg Config, plan Plan, body string, draft bool) error
	SetMergeability(ctx context.Context, cfg Config, headSHA string, mergeable bool, description string) error
}

// GHPublisher uses the authenticated gh CLI, keeping credentials out of args.
type GHPublisher struct {
	Runner CommandRunner
}

func (p GHPublisher) runner() CommandRunner {
	if p.Runner != nil {
		return p.Runner
	}
	return execCommandRunner{}
}

func (p GHPublisher) UpsertPR(ctx context.Context, cfg Config, plan Plan, body string, draft bool) error {
	runner := p.runner()
	result, err := runChecked(ctx, runner, cfg.RepoDir, "gh", "pr", "list",
		"--repo", cfg.OwnerRepo,
		"--head", cfg.SyncBranch,
		"--base", cfg.BaseBranch,
		"--state", "open",
		"--json", "number",
		"--jq", ".[0].number",
	)
	if err != nil {
		return fmt.Errorf("list sync pull requests: %w", err)
	}
	prNumber := strings.TrimSpace(result.Stdout)
	if prNumber == "" || prNumber == "null" {
		shortSHA := plan.UpstreamSHA
		if len(shortSHA) > 12 {
			shortSHA = shortSHA[:12]
		}
		args := []string{
			"api", "--method", "POST", fmt.Sprintf("repos/%s/pulls", cfg.OwnerRepo),
			"--raw-field", "title=NO-JIRA: sync with upstream " + shortSHA,
			"--raw-field", "body=" + body,
			"--raw-field", "head=" + cfg.SyncBranch,
			"--raw-field", "base=" + cfg.BaseBranch,
		}
		if draft {
			args = append(args, "--field", "draft=true")
		}
		if _, err := runChecked(ctx, runner, cfg.RepoDir, "gh", args...); err != nil {
			return fmt.Errorf("create sync pull request: %w", err)
		}
		return nil
	}

	view, err := runChecked(ctx, runner, cfg.RepoDir, "gh", "pr", "view", prNumber,
		"--repo", cfg.OwnerRepo, "--json", "isDraft", "--jq", ".isDraft")
	if err != nil {
		return fmt.Errorf("inspect sync pull request #%s draft state: %w", prNumber, err)
	}
	var isDraft bool
	switch strings.TrimSpace(view.Stdout) {
	case "true":
		isDraft = true
	case "false":
	default:
		return fmt.Errorf("inspect sync pull request #%s draft state: unexpected value %q", prNumber, strings.TrimSpace(view.Stdout))
	}
	if draft && !isDraft {
		if _, err := runChecked(ctx, runner, cfg.RepoDir, "gh", "pr", "ready", prNumber, "--repo", cfg.OwnerRepo, "--undo"); err != nil {
			return fmt.Errorf("convert sync pull request #%s to draft: %w", prNumber, err)
		}
	}
	if _, err := runChecked(ctx, runner, cfg.RepoDir, "gh", "api", "--method", "PATCH",
		fmt.Sprintf("repos/%s/pulls/%s", cfg.OwnerRepo, prNumber),
		"--raw-field", "body="+body,
	); err != nil {
		return fmt.Errorf("update sync pull request #%s: %w", prNumber, err)
	}
	if !draft && isDraft {
		if _, err := runChecked(ctx, runner, cfg.RepoDir, "gh", "pr", "ready", prNumber, "--repo", cfg.OwnerRepo); err != nil {
			return fmt.Errorf("mark sync pull request #%s ready: %w", prNumber, err)
		}
	}
	return nil
}

func (p GHPublisher) SetMergeability(ctx context.Context, cfg Config, headSHA string, mergeable bool, description string) error {
	state := "failure"
	if mergeable {
		state = "success"
	}
	_, err := runChecked(ctx, p.runner(), cfg.RepoDir, "gh", "api", "--method", "POST",
		fmt.Sprintf("repos/%s/statuses/%s", cfg.OwnerRepo, headSHA),
		"--field", "state="+state,
		"--field", "context="+MergeabilityContext,
		"--field", "description="+description,
	)
	if err != nil {
		return fmt.Errorf("publish %s status for %s: %w", MergeabilityContext, headSHA, err)
	}
	return nil
}
