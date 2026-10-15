package upstreamsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// PostProcessor regenerates repository artifacts after a clean upstream merge.
type PostProcessor interface {
	Run(ctx context.Context, repoDir string) error
}

// MakePostProcessor preserves the existing tidy/vendor/docs/snapshot steps.
type MakePostProcessor struct {
	Runner CommandRunner
}

func (p MakePostProcessor) Run(ctx context.Context, repoDir string) error {
	runner := p.Runner
	if runner == nil {
		runner = execCommandRunner{}
	}
	for _, command := range [][]string{
		{"go", "mod", "tidy"},
		{"go", "mod", "vendor"},
		{"make", "update-readme-tools"},
		{"make", "test-update-snapshots"},
	} {
		if _, err := runChecked(ctx, runner, repoDir, command[0], command[1:]...); err != nil {
			return fmt.Errorf("run %s: %w", strings.Join(command, " "), err)
		}
	}
	return nil
}

// Report is the publish result printed by the command and used by tests.
type Report struct {
	Plan          Plan   `json:"plan"`
	PublishedHead string `json:"publishedHead,omitempty"`
	PRDraft       bool   `json:"prDraft"`
	Mergeable     bool   `json:"mergeable"`
	Message       string `json:"message"`
}

// Publish applies a plan, pushes only with an expected-old-SHA lease, and
// updates the sync PR and required mergeability status.
func (s *Syncer) Publish(ctx context.Context, input Config, plan Plan) (Report, error) {
	cfg, err := normalizeConfig(input)
	if err != nil {
		return Report{}, err
	}
	if s.Git == nil {
		return Report{}, fmt.Errorf("Git runner is not configured")
	}
	if s.Publisher == nil {
		return Report{}, fmt.Errorf("PR publisher is not configured")
	}
	if plan.Action == ActionNoop {
		return Report{Plan: plan, Message: "downstream is already up to date"}, nil
	}
	status, err := runChecked(ctx, s.Git, cfg.RepoDir, "git", "status", "--porcelain")
	if err != nil {
		return Report{}, fmt.Errorf("check worktree status before publish: %w", err)
	}
	if strings.TrimSpace(status.Stdout) != "" {
		return Report{}, fmt.Errorf("refusing to publish from a dirty worktree")
	}

	var headSHA string
	draft := false
	message := "upstream sync branch updated"
	switch plan.Action {
	case ActionConflictCreatePR, ActionConflictFastForward, ActionConflictAdvanceBranch, ActionConflictPreserveBranch, ActionConflictRegenerate:
		draft = true
		message = "merge conflicts or unresolved branch changes remain; PR is draft"
		var regenerated []string
		var resolutionNote string
		var resolutionFailed bool
		headSHA, regenerated, resolutionNote, resolutionFailed, err = s.publishConflictHead(ctx, cfg, plan)
		plan.AutoRegeneratedPaths = regenerated
		plan.AutoResolutionFailed = resolutionFailed
		plan.ResolutionNote = resolutionNote
	case ActionCleanSync:
		headSHA, err = s.publishCleanHead(ctx, cfg, plan)
	default:
		return Report{}, fmt.Errorf("unsupported sync action %q", plan.Action)
	}
	if err != nil {
		return Report{Plan: plan, PublishedHead: headSHA, PRDraft: draft, Message: message}, err
	}
	staleErr := verifyPinnedRefs(ctx, s.Git, cfg, plan)
	mergeable := false
	description := "source refs moved during publish"
	verificationErr := staleErr
	if staleErr == nil {
		mergeable, description, verificationErr = s.mergeability(ctx, cfg, plan, headSHA)
		if verificationErr != nil {
			description = "mergeability verification failed"
			draft = true
			message = "mergeability verification failed; PR remains blocked"
		}
	}
	if staleErr != nil {
		draft = true
		message = "source refs moved during publish; PR remains blocked pending a fresh sync"
	}
	if plan.AutoResolutionFailed {
		mergeable = false
		description = "generated conflict resolution failed; existing branch was preserved"
		message = "generated conflict resolution failed; PR remains blocked"
		draft = true
	}
	statusErr := s.Publisher.SetMergeability(ctx, cfg, headSHA, mergeable, description)
	body, err := s.pullRequestBody(ctx, cfg, plan, headSHA, mergeable)
	if err != nil {
		return Report{Plan: plan, PublishedHead: headSHA, PRDraft: draft, Mergeable: mergeable}, errors.Join(verificationErr, statusErr, err)
	}
	if staleErr != nil {
		body += "\n\n⚠️ Source refs moved after planning; rerun the sync before marking this PR ready.\n"
	}
	upsertErr := s.Publisher.UpsertPR(ctx, cfg, plan, body, draft)
	if upsertErr != nil {
		upsertErr = fmt.Errorf("upsert sync pull request: %w", upsertErr)
	}
	if statusErr != nil {
		statusErr = fmt.Errorf("publish mergeability status: %w", statusErr)
	}
	report := Report{Plan: plan, PublishedHead: headSHA, PRDraft: draft, Mergeable: mergeable, Message: message}
	if err := errors.Join(verificationErr, upsertErr, statusErr); err != nil {
		return report, err
	}
	return report, nil
}

func (s *Syncer) publishConflictHead(ctx context.Context, cfg Config, plan Plan) (string, []string, string, bool, error) {
	headSHA := plan.SyncBranchSHA
	var regenerated []string
	var resolutionNote string
	resolutionFailed := false
	switch plan.Action {
	case ActionConflictCreatePR, ActionConflictFastForward:
		headSHA = plan.UpstreamSHA
	case ActionConflictAdvanceBranch:
		var err error
		headSHA, _, err = s.mergeInWorktree(ctx, cfg, headSHA, plan.UpstreamSHA, false, false)
		if err != nil {
			return plan.SyncBranchSHA, nil, "", false, fmt.Errorf("advance existing conflict branch: %w", err)
		}
	case ActionConflictRegenerate:
		baseSHA := plan.DownstreamSHA
		if plan.SyncBranchSHA != "" {
			headIsInUpstream, err := isAncestor(ctx, s.Git, cfg.RepoDir, plan.SyncBranchSHA, plan.UpstreamSHA)
			if err != nil {
				return plan.SyncBranchSHA, nil, "", false, fmt.Errorf("check existing conflict branch ancestry: %w", err)
			}
			if !headIsInUpstream {
				baseSHA = plan.SyncBranchSHA
			}
		}
		candidate, paths, err := s.mergeInWorktree(ctx, cfg, baseSHA, plan.UpstreamSHA, true, true)
		if err == nil {
			headSHA = candidate
			regenerated = union(paths, plan.ConflictPaths)
			resolutionNote = "Allowlisted generated artifacts were regenerated in a temporary worktree."
		} else {
			resolutionFailed = true
			resolutionNote = "Automatic generated-file resolution did not complete; the existing branch was preserved. Inspect local command errors before retrying."
			if plan.SyncBranchSHA == "" {
				headSHA = plan.UpstreamSHA
			} else {
				headSHA = plan.SyncBranchSHA
			}
		}
	case ActionConflictPreserveBranch:
		headSHA = plan.SyncBranchSHA
	default:
		return "", nil, "", false, fmt.Errorf("unexpected conflict action %q", plan.Action)
	}
	if headSHA == "" {
		return "", nil, resolutionNote, resolutionFailed, fmt.Errorf("conflict plan has no valid branch head")
	}
	if headSHA != plan.SyncBranchSHA {
		if err := verifyPinnedRefs(ctx, s.Git, cfg, plan); err != nil {
			return "", nil, resolutionNote, resolutionFailed, err
		}
		if err := pushWithLease(ctx, s.Git, cfg, headSHA, plan.SyncBranchSHA); err != nil {
			return "", nil, resolutionNote, resolutionFailed, err
		}
		if err := waitForRemoteHead(ctx, s.Git, cfg, headSHA); err != nil {
			return headSHA, regenerated, resolutionNote, resolutionFailed, err
		}
	}
	return headSHA, regenerated, resolutionNote, resolutionFailed, nil
}

func (s *Syncer) publishCleanHead(ctx context.Context, cfg Config, plan Plan) (string, error) {
	start := plan.DownstreamSHA
	if plan.SyncBranchSHA != "" {
		start = plan.SyncBranchSHA
	}
	headSHA, _, err := s.mergeInWorktree(ctx, cfg, start, plan.UpstreamSHA, true, false)
	if err != nil {
		return "", fmt.Errorf("integrate upstream in temporary worktree: %w", err)
	}
	if err := verifyPinnedRefs(ctx, s.Git, cfg, plan); err != nil {
		return "", err
	}
	if err := pushWithLease(ctx, s.Git, cfg, headSHA, plan.SyncBranchSHA); err != nil {
		return "", err
	}
	if err := waitForRemoteHead(ctx, s.Git, cfg, headSHA); err != nil {
		return headSHA, err
	}
	return headSHA, nil
}

func (s *Syncer) mergeInWorktree(ctx context.Context, cfg Config, baseSHA, upstreamSHA string, generate, allowGeneratedConflicts bool) (_ string, regenerated []string, retErr error) {
	expectedTree, conflicts, output, err := mergePreview(ctx, s.Git, cfg.RepoDir, baseSHA, upstreamSHA)
	if err != nil {
		return "", nil, err
	}
	if conflicts && !allowGeneratedConflicts {
		return "", nil, fmt.Errorf("merge preview conflicts: %s", output)
	}
	root, err := os.MkdirTemp("", "upstream-sync-")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary worktree directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(root); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove temporary worktree directory: %w", err))
		}
	}()
	worktree := filepath.Join(root, "repo")
	if _, err := runChecked(ctx, s.Git, cfg.RepoDir, "git", "worktree", "add", "--detach", worktree, baseSHA); err != nil {
		return "", nil, fmt.Errorf("create temporary Git worktree: %w", err)
	}
	defer func() {
		if _, err := s.Git.Run(context.Background(), cfg.RepoDir, "git", "worktree", "remove", "--force", worktree); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("remove temporary Git worktree: %w", err))
		}
	}()
	mergeResult, mergeErr := s.Git.Run(ctx, worktree, "git", "merge", "--no-ff", upstreamSHA, "-m", "chore: merge upstream changes")
	if mergeErr != nil || mergeResult.ExitCode != 0 {
		unmerged, listErr := unmergedPaths(ctx, s.Git, worktree)
		if listErr != nil || !allowGeneratedConflicts || len(unmerged) == 0 || !allConflictsGenerated(unmerged) {
			_, _ = s.Git.Run(context.Background(), worktree, "git", "merge", "--abort")
			if mergeErr != nil {
				return "", nil, fmt.Errorf("merge pinned upstream SHA: %w", mergeErr)
			}
			return "", nil, fmt.Errorf("merge pinned upstream SHA exited %d: %s", mergeResult.ExitCode, strings.TrimSpace(mergeResult.Stderr+mergeResult.Stdout))
		}
		regenerated, err = resolveGeneratedConflicts(ctx, s.Git, worktree, unmerged)
		if err != nil {
			_, _ = s.Git.Run(context.Background(), worktree, "git", "merge", "--abort")
			return "", nil, err
		}
		remaining, err := unmergedPaths(ctx, s.Git, worktree)
		if err != nil || len(remaining) != 0 {
			_, _ = s.Git.Run(context.Background(), worktree, "git", "merge", "--abort")
			return "", nil, fmt.Errorf("generated conflict resolution left unmerged paths: %v", remaining)
		}
		if _, err := runChecked(ctx, s.Git, worktree, "git", "commit", "--no-edit"); err != nil {
			return "", nil, fmt.Errorf("commit generated conflict resolutions in temporary worktree: %w", err)
		}
	} else {
		if conflicts {
			return "", nil, fmt.Errorf("merge-tree reported conflicts but git merge completed without conflict markers")
		}
		actualTree, err := gitOutput(ctx, s.Git, worktree, "write-tree")
		if err != nil {
			return "", nil, fmt.Errorf("write integrated tree: %w", err)
		}
		if actualTree != expectedTree {
			return "", nil, fmt.Errorf("merge result tree %s differs from preflight tree %s", actualTree, expectedTree)
		}
	}
	if generate {
		processor := s.PostProcess
		if processor == nil {
			processor = MakePostProcessor{Runner: s.Git}
		}
		if err := processor.Run(ctx, worktree); err != nil {
			return "", nil, fmt.Errorf("regenerate sync artifacts: %w", err)
		}
		if err := commitGenerated(ctx, s.Git, worktree); err != nil {
			return "", nil, err
		}
	}
	headSHA, err := gitOutput(ctx, s.Git, worktree, "rev-parse", "HEAD")
	if err != nil {
		return "", nil, fmt.Errorf("read integrated branch head: %w", err)
	}
	containsUpstream, err := isAncestor(ctx, s.Git, worktree, upstreamSHA, headSHA)
	if err != nil {
		return "", nil, err
	}
	if !containsUpstream {
		return "", nil, fmt.Errorf("integrated branch does not contain pinned upstream SHA %s", upstreamSHA)
	}
	return headSHA, regenerated, nil
}

func commitGenerated(ctx context.Context, runner CommandRunner, worktree string) error {
	status, err := gitOutput(ctx, runner, worktree, "status", "--porcelain")
	if err != nil {
		return fmt.Errorf("inspect generated changes: %w", err)
	}
	if status == "" {
		return nil
	}
	if _, err := runChecked(ctx, runner, worktree, "git", "add", "--", "go.mod", "go.sum", "vendor/", "README.md", "docs/configuration.md", "pkg/mcp/testdata/"); err != nil {
		return fmt.Errorf("stage generated sync artifacts in temporary worktree: %w", err)
	}
	if _, err := runChecked(ctx, runner, worktree, "git", "commit", "-m", "chore: regenerate files for upstream sync"); err != nil {
		return fmt.Errorf("commit generated sync artifacts in temporary worktree: %w", err)
	}
	remaining, err := gitOutput(ctx, runner, worktree, "status", "--porcelain")
	if err != nil {
		return err
	}
	if remaining != "" {
		return fmt.Errorf("generated files outside the sync allowlist changed: %s", remaining)
	}
	return nil
}

func pushWithLease(ctx context.Context, runner CommandRunner, cfg Config, headSHA, expectedSHA string) error {
	if _, err := runChecked(ctx, runner, cfg.RepoDir, "git", "push",
		"--force-with-lease=refs/heads/"+cfg.SyncBranch+":"+expectedSHA,
		cfg.DownstreamRemote,
		headSHA+":refs/heads/"+cfg.SyncBranch,
	); err != nil {
		return fmt.Errorf("push sync branch with lease: %w", err)
	}
	return nil
}

func verifyPinnedRefs(ctx context.Context, runner CommandRunner, cfg Config, plan Plan) error {
	refs := []struct {
		remote string
		branch string
		want   string
	}{
		{remote: cfg.UpstreamRemote, branch: cfg.BaseBranch, want: plan.UpstreamSHA},
		{remote: cfg.DownstreamRemote, branch: cfg.BaseBranch, want: plan.DownstreamSHA},
	}
	for _, ref := range refs {
		result, err := runChecked(ctx, runner, cfg.RepoDir, "git", "ls-remote", "--heads", ref.remote, "refs/heads/"+ref.branch)
		if err != nil {
			return fmt.Errorf("recheck %s/%s before publish: %w", ref.remote, ref.branch, err)
		}
		fields := strings.Fields(result.Stdout)
		if len(fields) == 0 || fields[0] != ref.want {
			return fmt.Errorf("%s/%s moved since planning (expected %s, found %s); rerun plan", ref.remote, ref.branch, ref.want, firstField(fields))
		}
	}
	return nil
}

func firstField(fields []string) string {
	if len(fields) == 0 {
		return "missing"
	}
	return fields[0]
}

func waitForRemoteHead(ctx context.Context, runner CommandRunner, cfg Config, expectedSHA string) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for attempt := 0; attempt < 30; attempt++ {
		result, err := runChecked(ctx, runner, cfg.RepoDir, "git", "ls-remote", "--heads", cfg.DownstreamRemote, "refs/heads/"+cfg.SyncBranch)
		if err == nil && strings.HasPrefix(result.Stdout, expectedSHA+"\t") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for sync branch indexing: %w", ctx.Err())
		case <-ticker.C:
		}
	}
	return fmt.Errorf("remote sync branch did not reach expected SHA %s", expectedSHA)
}

func (s *Syncer) mergeability(ctx context.Context, cfg Config, plan Plan, headSHA string) (bool, string, error) {
	containsUpstream, err := isAncestor(ctx, s.Git, cfg.RepoDir, plan.UpstreamSHA, headSHA)
	if err != nil {
		return false, "could not verify upstream ancestry", err
	}
	if !containsUpstream {
		return false, "latest upstream commit is not included", nil
	}
	_, conflicts, _, err := mergePreview(ctx, s.Git, cfg.RepoDir, plan.DownstreamSHA, headSHA)
	if err != nil {
		return false, "could not preflight PR head against downstream", err
	}
	if conflicts {
		return false, "PR head still conflicts with downstream", nil
	}
	return true, "PR head includes upstream and merges cleanly with downstream", nil
}

func (s *Syncer) pullRequestBody(ctx context.Context, cfg Config, plan Plan, headSHA string, mergeable bool) (string, error) {
	changes, err := gitOutput(ctx, s.Git, cfg.RepoDir, "log", "--pretty=format:- %h %s (%an)", plan.DownstreamSHA+".."+plan.UpstreamSHA)
	if err != nil {
		return "", fmt.Errorf("build upstream changelog: %w", err)
	}
	var body strings.Builder
	if plan.TargetConflicts || plan.BranchConflicts {
		body.WriteString("### ⚠️ Upstream Sync — conflict review required\n\n")
		if len(plan.AutoRegeneratedPaths) == 0 {
			body.WriteString("No conflicting hunks were automatically resolved. This PR remains draft until the mergeability check passes.\n\n")
		} else {
			body.WriteString("Conflicts were limited to allowlisted generated artifacts, which were regenerated in a temporary worktree. This PR remains draft for review.\n\n")
			for _, path := range plan.AutoRegeneratedPaths {
				fmt.Fprintf(&body, "- Regenerated: `%s`\n", path)
			}
			body.WriteString("\n")
		}
		if plan.ResolutionNote != "" {
			fmt.Fprintf(&body, "**Generated-conflict handling:** %s\n\n", plan.ResolutionNote)
		}
		fmt.Fprintf(&body, "- Upstream SHA: `%s`\n- Downstream SHA: `%s`\n- Published head: `%s`\n", plan.UpstreamSHA, plan.DownstreamSHA, headSHA)
		containsUpstream, err := isAncestor(ctx, s.Git, cfg.RepoDir, plan.UpstreamSHA, headSHA)
		if err != nil {
			return "", fmt.Errorf("check upstream ancestry for PR body: %w", err)
		}
		if plan.SyncBranchSHA != "" && !containsUpstream {
			body.WriteString("- Latest upstream changes are pending; the existing branch was preserved.\n")
		}
		if len(plan.ConflictPaths) != 0 {
			body.WriteString("\n**Conflicting paths:**\n")
			for _, path := range plan.ConflictPaths {
				fmt.Fprintf(&body, "- `%s`\n", path)
			}
		}
		if plan.ConflictOutput != "" {
			fmt.Fprintf(&body, "\n<details><summary>Merge preflight output</summary>\n\n```text\n%s\n```\n</details>\n", plan.ConflictOutput)
		}
	} else {
		body.WriteString("### 🔄 Upstream Sync\n\nThis PR syncs the fork with the latest upstream changes.\n\n")
		fmt.Fprintf(&body, "- Upstream SHA: `%s`\n- Downstream SHA: `%s`\n- Mergeability: `%t`\n\n**Changes:**\n%s\n", plan.UpstreamSHA, plan.DownstreamSHA, mergeable, changes)
	}
	return body.String(), nil
}
