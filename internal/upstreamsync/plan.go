package upstreamsync

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
)

// Config identifies the source, downstream target, and persistent sync PR.
type Config struct {
	RepoDir          string
	UpstreamRemote   string
	UpstreamRepo     string
	DownstreamRemote string
	DownstreamRepo   string
	BaseBranch       string
	SyncBranch       string
	OwnerRepo        string
}

const syncBaseBranch = "main"

// Action is the safe next action inferred from pinned refs and merge previews.
type Action string

const (
	ActionNoop                   Action = "noop"
	ActionCleanSync              Action = "clean-sync"
	ActionConflictCreatePR       Action = "conflict-create-pr"
	ActionConflictFastForward    Action = "conflict-fast-forward"
	ActionConflictAdvanceBranch  Action = "conflict-advance-branch"
	ActionConflictPreserveBranch Action = "conflict-preserve-branch"
	ActionConflictRegenerate     Action = "conflict-regenerate-generated"
)

// Plan is an immutable description of one synchronization attempt.
type Plan struct {
	UpstreamSHA          string   `json:"upstreamSHA"`
	DownstreamSHA        string   `json:"downstreamSHA"`
	MergeBaseSHA         string   `json:"mergeBaseSHA"`
	SyncBranchSHA        string   `json:"syncBranchSHA,omitempty"`
	BehindCount          int      `json:"behindCount"`
	ChangedPaths         []string `json:"changedPaths,omitempty"`
	OverlappingPaths     []string `json:"overlappingPaths,omitempty"`
	ConflictPaths        []string `json:"conflictPaths,omitempty"`
	ConflictOutput       string   `json:"conflictOutput,omitempty"`
	AutoRegeneratedPaths []string `json:"autoRegeneratedPaths,omitempty"`
	AutoResolutionFailed bool     `json:"autoResolutionFailed,omitempty"`
	ResolutionNote       string   `json:"resolutionNote,omitempty"`
	TargetConflicts      bool     `json:"targetConflicts"`
	BranchConflicts      bool     `json:"branchConflicts"`
	Action               Action   `json:"action"`
}

// Syncer coordinates plan, integration, and publication operations.
type Syncer struct {
	Git         CommandRunner
	Publisher   Publisher
	PostProcess PostProcessor
}

// New creates a syncer with the production Git and GitHub CLI adapters.
func New() *Syncer {
	return &Syncer{Git: execCommandRunner{}, Publisher: GHPublisher{}, PostProcess: MakePostProcessor{Runner: execCommandRunner{}}}
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.RepoDir == "" {
		cfg.RepoDir = "."
	}
	abs, err := filepath.Abs(cfg.RepoDir)
	if err != nil {
		return cfg, fmt.Errorf("resolve repository directory: %w", err)
	}
	cfg.RepoDir = abs
	if cfg.UpstreamRemote == "" {
		cfg.UpstreamRemote = "upstream"
	}
	if cfg.DownstreamRemote == "" {
		cfg.DownstreamRemote = "origin"
	}
	if cfg.BaseBranch == "" {
		cfg.BaseBranch = syncBaseBranch
	}
	if cfg.BaseBranch != syncBaseBranch {
		return cfg, fmt.Errorf("upstream synchronization only supports %q, got %q", syncBaseBranch, cfg.BaseBranch)
	}
	if cfg.SyncBranch == "" {
		cfg.SyncBranch = "upstream-sync"
	}
	if cfg.UpstreamRepo == "" {
		cfg.UpstreamRepo = "containers/kubernetes-mcp-server"
	}
	if cfg.DownstreamRepo == "" {
		cfg.DownstreamRepo = cfg.OwnerRepo
	}
	if cfg.OwnerRepo == "" {
		cfg.OwnerRepo = cfg.DownstreamRepo
	}
	if cfg.OwnerRepo == "" {
		return cfg, fmt.Errorf("downstream GitHub repository is required")
	}
	if cfg.OwnerRepo != cfg.DownstreamRepo {
		return cfg, fmt.Errorf("PR repository %q must match the downstream push repository %q", cfg.OwnerRepo, cfg.DownstreamRepo)
	}
	if cfg.UpstreamRemote == cfg.DownstreamRemote {
		return cfg, fmt.Errorf("upstream and downstream remotes must differ")
	}
	return cfg, nil
}

// BuildPlan fetches and pins the source refs, then classifies mergeability.
func (s *Syncer) BuildPlan(ctx context.Context, input Config) (Plan, error) {
	cfg, err := normalizeConfig(input)
	if err != nil {
		return Plan{}, err
	}
	if s.Git == nil {
		return Plan{}, fmt.Errorf("Git runner is not configured")
	}
	if err := validateRemote(ctx, s.Git, cfg.RepoDir, cfg.UpstreamRemote, cfg.UpstreamRepo); err != nil {
		return Plan{}, err
	}
	if err := validateRemote(ctx, s.Git, cfg.RepoDir, cfg.DownstreamRemote, cfg.DownstreamRepo); err != nil {
		return Plan{}, err
	}

	upstreamSHA, err := fetchSHA(ctx, s.Git, cfg.RepoDir, cfg.UpstreamRemote, cfg.BaseBranch)
	if err != nil {
		return Plan{}, fmt.Errorf("fetch upstream %s/%s: %w", cfg.UpstreamRemote, cfg.BaseBranch, err)
	}
	downstreamSHA, err := fetchSHA(ctx, s.Git, cfg.RepoDir, cfg.DownstreamRemote, cfg.BaseBranch)
	if err != nil {
		return Plan{}, fmt.Errorf("fetch downstream %s/%s: %w", cfg.DownstreamRemote, cfg.BaseBranch, err)
	}
	mergeBase, err := gitOutput(ctx, s.Git, cfg.RepoDir, "merge-base", downstreamSHA, upstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("find upstream/downstream merge base: %w", err)
	}
	behindRaw, err := gitOutput(ctx, s.Git, cfg.RepoDir, "rev-list", "--count", downstreamSHA+".."+upstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("count upstream commits: %w", err)
	}
	var behind int
	if _, err := fmt.Sscanf(strings.TrimSpace(behindRaw), "%d", &behind); err != nil {
		return Plan{}, fmt.Errorf("parse upstream commit count %q: %w", behindRaw, err)
	}
	plan := Plan{UpstreamSHA: upstreamSHA, DownstreamSHA: downstreamSHA, MergeBaseSHA: mergeBase, BehindCount: behind}
	if behind == 0 {
		plan.Action = ActionNoop
		return plan, nil
	}

	upstreamPaths, err := gitOutput(ctx, s.Git, cfg.RepoDir, "diff", "--name-only", mergeBase, upstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("list upstream changed paths: %w", err)
	}
	downstreamPaths, err := gitOutput(ctx, s.Git, cfg.RepoDir, "diff", "--name-only", mergeBase, downstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("list downstream changed paths: %w", err)
	}
	plan.ChangedPaths, plan.OverlappingPaths = comparePaths(upstreamPaths, downstreamPaths)

	targetTree, targetConflicts, targetOutput, err := mergePreview(ctx, s.Git, cfg.RepoDir, downstreamSHA, upstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("preflight downstream/upstream merge: %w", err)
	}
	_ = targetTree // The tree is retained for verification during apply in future refinements.
	plan.TargetConflicts = targetConflicts
	if targetConflicts {
		plan.ConflictOutput = targetOutput
		plan.ConflictPaths = parseConflictPaths(targetOutput)
	}

	syncSHA, err := remoteBranchSHA(ctx, s.Git, cfg.RepoDir, cfg.DownstreamRemote, cfg.SyncBranch)
	if err != nil {
		return Plan{}, fmt.Errorf("inspect existing sync branch: %w", err)
	}
	plan.SyncBranchSHA = syncSHA
	if syncSHA == "" {
		if targetConflicts {
			if allConflictsGenerated(plan.ConflictPaths) {
				plan.Action = ActionConflictRegenerate
			} else {
				plan.Action = ActionConflictCreatePR
			}
		} else {
			plan.Action = ActionCleanSync
		}
		return plan, nil
	}

	if targetConflicts {
		isAncestor, err := isAncestor(ctx, s.Git, cfg.RepoDir, syncSHA, upstreamSHA)
		if err != nil {
			return Plan{}, fmt.Errorf("check sync head ancestry: %w", err)
		}
		if isAncestor {
			if allConflictsGenerated(plan.ConflictPaths) {
				plan.Action = ActionConflictRegenerate
			} else {
				plan.Action = ActionConflictFastForward
			}
			return plan, nil
		}
		_, branchConflicts, branchOutput, err := mergePreview(ctx, s.Git, cfg.RepoDir, syncSHA, upstreamSHA)
		if err != nil {
			return Plan{}, fmt.Errorf("preflight update to existing sync branch: %w", err)
		}
		plan.BranchConflicts = branchConflicts
		if branchConflicts {
			plan.ConflictOutput += "\nExisting sync branch update also conflicts:\n" + branchOutput
			plan.ConflictPaths = union(plan.ConflictPaths, parseConflictPaths(branchOutput))
			if allConflictsGenerated(plan.ConflictPaths) {
				plan.Action = ActionConflictRegenerate
			} else {
				plan.Action = ActionConflictPreserveBranch
			}
		} else {
			if allConflictsGenerated(plan.ConflictPaths) {
				plan.Action = ActionConflictRegenerate
			} else {
				plan.Action = ActionConflictAdvanceBranch
			}
		}
		return plan, nil
	}

	_, branchConflicts, branchOutput, err := mergePreview(ctx, s.Git, cfg.RepoDir, syncSHA, upstreamSHA)
	if err != nil {
		return Plan{}, fmt.Errorf("preflight update to existing sync branch: %w", err)
	}
	if branchConflicts {
		plan.BranchConflicts = true
		plan.ConflictOutput = branchOutput
		plan.ConflictPaths = parseConflictPaths(branchOutput)
		if allConflictsGenerated(plan.ConflictPaths) {
			plan.Action = ActionConflictRegenerate
		} else {
			plan.Action = ActionConflictPreserveBranch
		}
		return plan, nil
	}
	plan.Action = ActionCleanSync
	return plan, nil
}

func validateRemote(ctx context.Context, runner CommandRunner, dir, remote, expected string) error {
	url, err := gitOutput(ctx, runner, dir, "remote", "get-url", remote)
	if err != nil {
		return fmt.Errorf("read remote %q URL: %w", remote, err)
	}
	if expected == "" {
		return nil
	}
	actual := githubRepoPath(url)
	want := strings.TrimSuffix(strings.TrimPrefix(expected, "github.com/"), ".git")
	if actual == "" || actual != want {
		return fmt.Errorf("remote %q does not identify expected GitHub repository %q", remote, expected)
	}
	return nil
}

func githubRepoPath(raw string) string {
	raw = strings.TrimSpace(raw)
	if strings.HasPrefix(raw, "git@github.com:") {
		return strings.TrimSuffix(strings.TrimPrefix(raw, "git@github.com:"), ".git")
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() != "github.com" {
		return ""
	}
	if parsed.Scheme != "https" && parsed.Scheme != "ssh" {
		return ""
	}
	return strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), ".git")
}

func fetchSHA(ctx context.Context, runner CommandRunner, dir, remote, branch string) (string, error) {
	if _, err := runChecked(ctx, runner, dir, "git", "fetch", "--no-tags", remote, "refs/heads/"+branch); err != nil {
		return "", err
	}
	return gitOutput(ctx, runner, dir, "rev-parse", "FETCH_HEAD")
}

func remoteBranchSHA(ctx context.Context, runner CommandRunner, dir, remote, branch string) (string, error) {
	result, err := runChecked(ctx, runner, dir, "git", "ls-remote", "--heads", remote, "refs/heads/"+branch)
	if err != nil {
		return "", err
	}
	fields := strings.Fields(result.Stdout)
	if len(fields) == 0 {
		return "", nil
	}
	remoteSHA := fields[0]
	if _, err := runChecked(ctx, runner, dir, "git", "fetch", "--no-tags", remote, "refs/heads/"+branch); err != nil {
		return "", err
	}
	fetchedSHA, err := gitOutput(ctx, runner, dir, "rev-parse", "FETCH_HEAD")
	if err != nil {
		return "", err
	}
	if fetchedSHA != remoteSHA {
		return "", fmt.Errorf("remote sync branch changed while being inspected; retry")
	}
	return remoteSHA, nil
}

func mergePreview(ctx context.Context, runner CommandRunner, dir, left, right string) (string, bool, string, error) {
	result, err := runner.Run(ctx, dir, "git", "merge-tree", "--write-tree", left, right)
	output := strings.TrimSpace(result.Stdout + result.Stderr)
	if err == nil && result.ExitCode == 0 {
		first := strings.Fields(result.Stdout)
		if len(first) == 0 {
			return "", false, output, fmt.Errorf("merge-tree returned no tree id")
		}
		return first[0], false, output, nil
	}
	if result.ExitCode == 1 {
		return "", true, output, nil
	}
	if err != nil {
		return "", false, output, err
	}
	return "", false, output, fmt.Errorf("merge-tree exited %d: %s", result.ExitCode, output)
}

func isAncestor(ctx context.Context, runner CommandRunner, dir, ancestor, descendant string) (bool, error) {
	result, err := runner.Run(ctx, dir, "git", "merge-base", "--is-ancestor", ancestor, descendant)
	if err == nil && result.ExitCode == 0 {
		return true, nil
	}
	if result.ExitCode == 1 {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return false, fmt.Errorf("merge-base exited %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr+result.Stdout))
}

func gitOutput(ctx context.Context, runner CommandRunner, dir string, args ...string) (string, error) {
	result, err := runChecked(ctx, runner, dir, "git", args...)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(result.Stdout), nil
}

func comparePaths(upstream, downstream string) ([]string, []string) {
	up := make(map[string]struct{})
	down := make(map[string]struct{})
	for _, path := range strings.Split(upstream, "\n") {
		if path = strings.TrimSpace(path); path != "" {
			up[path] = struct{}{}
		}
	}
	for _, path := range strings.Split(downstream, "\n") {
		if path = strings.TrimSpace(path); path != "" {
			down[path] = struct{}{}
		}
	}
	changed := make([]string, 0, len(up))
	overlap := make([]string, 0)
	for path := range up {
		changed = append(changed, path)
		if _, ok := down[path]; ok {
			overlap = append(overlap, path)
		}
	}
	sort.Strings(changed)
	sort.Strings(overlap)
	return changed, overlap
}

func parseConflictPaths(output string) []string {
	var paths []string
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, "CONFLICT") {
			continue
		}
		if i := strings.LastIndex(line, " in "); i >= 0 {
			paths = append(paths, strings.Trim(strings.TrimSpace(line[i+4:]), "`\""))
		}
	}
	return union(paths)
}

func union(values ...[]string) []string {
	set := make(map[string]struct{})
	for _, group := range values {
		for _, value := range group {
			if value = strings.TrimSpace(value); value != "" {
				set[value] = struct{}{}
			}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
