package upstreamsync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeRunner struct {
	run   func(ctx context.Context, dir, name string, args ...string) (CommandResult, error)
	calls *[]string
}

func (f fakeRunner) Run(ctx context.Context, dir, name string, args ...string) (CommandResult, error) {
	if f.calls != nil {
		*f.calls = append(*f.calls, name+" "+strings.Join(args, " "))
	}
	return f.run(ctx, dir, name, args...)
}

type fakePublisher struct {
	draft       bool
	body        string
	headSHA     string
	status      bool
	description string
}

type fakePostProcessor struct {
	runs int
}

func (f *fakePostProcessor) Run(context.Context, string) error {
	f.runs++
	return nil
}

type failingPostProcessor struct{}

func (failingPostProcessor) Run(context.Context, string) error {
	return errors.New("synthetic generator failure")
}

func (f *fakePublisher) UpsertPR(_ context.Context, _ Config, plan Plan, body string, draft bool) error {
	f.body = body
	f.draft = draft
	f.headSHA = plan.UpstreamSHA
	return nil
}

func (f *fakePublisher) SetMergeability(_ context.Context, _ Config, _ string, mergeable bool, description string) error {
	f.status = mergeable
	f.description = description
	return nil
}

func testConfig() Config {
	return Config{
		RepoDir:          "/repo",
		UpstreamRemote:   "upstream",
		UpstreamRepo:     "containers/kubernetes-mcp-server",
		DownstreamRemote: "downstream",
		DownstreamRepo:   "openshift/openshift-mcp-server",
		OwnerRepo:        "openshift/openshift-mcp-server",
		BaseBranch:       "main",
		SyncBranch:       "upstream-sync",
	}
}

func plannerRunner(targetConflict bool, syncSHA string, branchConflict bool) fakeRunner {
	fetchHead := ""
	calls := []string{}
	return fakeRunner{calls: &calls, run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		joined := strings.Join(args, " ")
		switch {
		case name == "git" && joined == "status --porcelain":
			return CommandResult{}, nil
		case name == "git" && joined == "remote get-url upstream":
			return CommandResult{Stdout: "https://github.com/containers/kubernetes-mcp-server.git"}, nil
		case name == "git" && joined == "remote get-url downstream":
			return CommandResult{Stdout: "https://github.com/openshift/openshift-mcp-server.git"}, nil
		case name == "git" && joined == "fetch --no-tags upstream refs/heads/main":
			fetchHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			return CommandResult{}, nil
		case name == "git" && joined == "fetch --no-tags downstream refs/heads/main":
			fetchHead = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			return CommandResult{}, nil
		case name == "git" && joined == "rev-parse FETCH_HEAD":
			return CommandResult{Stdout: fetchHead}, nil
		case name == "git" && joined == "merge-base bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":
			return CommandResult{Stdout: "cccccccccccccccccccccccccccccccccccccccc"}, nil
		case name == "git" && joined == "rev-list --count bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":
			return CommandResult{Stdout: "2"}, nil
		case name == "git" && joined == "diff --name-only cccccccccccccccccccccccccccccccccccccccc aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":
			return CommandResult{Stdout: "shared.txt\nupstream.txt\n"}, nil
		case name == "git" && joined == "diff --name-only cccccccccccccccccccccccccccccccccccccccc bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb":
			return CommandResult{Stdout: "shared.txt\ndownstream.txt\n"}, nil
		case name == "git" && strings.HasPrefix(joined, "merge-tree --write-tree"):
			isConflict := targetConflict
			if strings.Contains(joined, "dddddddddddddddddddddddddddddddddddddddd") {
				isConflict = branchConflict
			}
			if isConflict {
				msg := "tree-id\nCONFLICT (content): Merge conflict in shared.txt\n"
				return CommandResult{Stdout: msg, ExitCode: 1}, &CommandError{ExitCode: 1, Output: msg}
			}
			return CommandResult{Stdout: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\n"}, nil
		case name == "git" && joined == "ls-remote --heads downstream refs/heads/upstream-sync":
			if syncSHA == "" {
				return CommandResult{}, nil
			}
			return CommandResult{Stdout: fmt.Sprintf("%s\trefs/heads/upstream-sync\n", syncSHA)}, nil
		case name == "git" && joined == "ls-remote --heads upstream refs/heads/main":
			return CommandResult{Stdout: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\trefs/heads/main\n"}, nil
		case name == "git" && joined == "ls-remote --heads downstream refs/heads/main":
			return CommandResult{Stdout: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\trefs/heads/main\n"}, nil
		case name == "git" && strings.HasPrefix(joined, "push "):
			refspec := args[len(args)-1]
			parts := strings.SplitN(refspec, ":", 2)
			if len(parts) == 2 {
				syncSHA = parts[0]
			}
			return CommandResult{}, nil
		case name == "git" && strings.HasPrefix(joined, "log --pretty=format:- %h %s (%an)"):
			return CommandResult{Stdout: "- aaaaaaa upstream change"}, nil
		case name == "git" && strings.HasPrefix(joined, "worktree add --detach "):
			return CommandResult{}, nil
		case name == "git" && joined == "merge --no-ff aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa -m chore: merge upstream changes":
			return CommandResult{}, nil
		case name == "git" && joined == "write-tree":
			return CommandResult{Stdout: "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}, nil
		case name == "git" && joined == "rev-parse HEAD":
			return CommandResult{Stdout: "ffffffffffffffffffffffffffffffffffffffff"}, nil
		case name == "git" && strings.HasPrefix(joined, "worktree remove --force "):
			return CommandResult{}, nil
		case name == "git" && joined == "fetch --no-tags downstream refs/heads/upstream-sync":
			fetchHead = syncSHA
			return CommandResult{}, nil
		case name == "git" && strings.HasPrefix(joined, "merge-base --is-ancestor"):
			if strings.Contains(joined, "dddddddddddddddddddddddddddddddddddddddd") && strings.Contains(joined, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") && syncSHA == "dddddddddddddddddddddddddddddddddddddddd" {
				return CommandResult{ExitCode: 1}, &CommandError{ExitCode: 1}
			}
			return CommandResult{}, nil
		default:
			return CommandResult{}, fmt.Errorf("unexpected command: %s %s", name, joined)
		}
	}}
}

func plannerRunnerWithConflictPath(path, syncSHA string) fakeRunner {
	runner := plannerRunner(true, syncSHA, false)
	original := runner.run
	runner.run = func(ctx context.Context, dir, name string, args ...string) (CommandResult, error) {
		result, err := original(ctx, dir, name, args...)
		if name == "git" && strings.HasPrefix(strings.Join(args, " "), "merge-tree --write-tree") {
			result.Stdout = strings.ReplaceAll(result.Stdout, "shared.txt", path)
			if commandErr, ok := err.(*CommandError); ok {
				commandErr.Output = strings.ReplaceAll(commandErr.Output, "shared.txt", path)
			}
		}
		return result, err
	}
	return runner
}

func TestBuildPlanReportsOverlapAndCleanMerge(t *testing.T) {
	syncer := &Syncer{Git: plannerRunner(false, "", false)}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionCleanSync {
		t.Fatalf("expected clean action, got %q", plan.Action)
	}
	if got := strings.Join(plan.OverlappingPaths, ","); got != "shared.txt" {
		t.Fatalf("unexpected overlaps: %s", got)
	}
	if plan.UpstreamSHA != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || plan.DownstreamSHA != "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" {
		t.Fatalf("plan did not preserve pinned refs: %#v", plan)
	}
}

func TestBuildPlanNoopStopsAfterFetchingMainRefs(t *testing.T) {
	var calls []string
	fetchHead := ""
	runner := fakeRunner{calls: &calls, run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		joined := strings.Join(args, " ")
		switch {
		case name == "git" && joined == "remote get-url upstream":
			return CommandResult{Stdout: "https://github.com/containers/kubernetes-mcp-server.git"}, nil
		case name == "git" && joined == "remote get-url downstream":
			return CommandResult{Stdout: "https://github.com/openshift/openshift-mcp-server.git"}, nil
		case name == "git" && joined == "fetch --no-tags upstream refs/heads/main":
			fetchHead = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
			return CommandResult{}, nil
		case name == "git" && joined == "fetch --no-tags downstream refs/heads/main":
			fetchHead = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
			return CommandResult{}, nil
		case name == "git" && joined == "rev-parse FETCH_HEAD":
			return CommandResult{Stdout: fetchHead}, nil
		case name == "git" && joined == "merge-base bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":
			return CommandResult{Stdout: "cccccccccccccccccccccccccccccccccccccccc"}, nil
		case name == "git" && joined == "rev-list --count bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa":
			return CommandResult{Stdout: "0"}, nil
		default:
			return CommandResult{}, fmt.Errorf("unexpected command in no-op plan: %s %s", name, joined)
		}
	}}
	plan, err := (&Syncer{Git: runner}).BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionNoop || plan.BehindCount != 0 {
		t.Fatalf("expected no-op plan, got %#v", plan)
	}
	for _, call := range calls {
		if strings.Contains(call, "merge-tree") || strings.Contains(call, "upstream-sync") {
			t.Fatalf("no-op plan performed unnecessary sync work: %s", call)
		}
	}
}

func TestNormalizeConfigRejectsPRTargetDifferentFromPushRemote(t *testing.T) {
	cfg := testConfig()
	cfg.OwnerRepo = "grokspawn/kubernetes-mcp-server"
	if _, err := normalizeConfig(cfg); err == nil {
		t.Fatal("expected mismatched downstream push/PR repositories to be rejected")
	}
}

func TestNormalizeConfigRejectsNonMainBaseBranch(t *testing.T) {
	cfg := testConfig()
	cfg.BaseBranch = "release-5.0"
	if _, err := normalizeConfig(cfg); err == nil {
		t.Fatal("expected non-main base branch to be rejected")
	}
}

func TestBuildPlanCreatesConflictPRAction(t *testing.T) {
	syncer := &Syncer{Git: plannerRunner(true, "", false)}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionConflictCreatePR {
		t.Fatalf("expected conflict-create-pr, got %q", plan.Action)
	}
	if !plan.TargetConflicts {
		t.Fatal("expected target conflict")
	}
	if !strings.Contains(strings.Join(plan.ConflictPaths, ","), "shared.txt") {
		t.Fatalf("conflict path was not reported: %#v", plan.ConflictPaths)
	}
}

func TestBuildPlanSelectsRegenerationOnlyForAllowlistedPaths(t *testing.T) {
	runner := plannerRunnerWithConflictPath("README.md", "")
	plan, err := (&Syncer{Git: runner}).BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionConflictRegenerate {
		t.Fatalf("expected generated conflict action, got %q", plan.Action)
	}
	if !allConflictsGenerated(plan.ConflictPaths) {
		t.Fatalf("README conflict should be allowlisted: %#v", plan.ConflictPaths)
	}
}

func TestGeneratedResolutionFailureFallsBackToDraftUpstreamHead(t *testing.T) {
	syncSHA := "dddddddddddddddddddddddddddddddddddddddd"
	runner := plannerRunnerWithConflictPath("README.md", syncSHA)
	publisher := &fakePublisher{}
	syncer := &Syncer{Git: runner, Publisher: publisher, PostProcess: failingPostProcessor{}}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionConflictRegenerate {
		t.Fatalf("expected generated conflict action, got %q", plan.Action)
	}
	report, err := syncer.Publish(context.Background(), testConfig(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.PublishedHead != syncSHA || !report.Plan.AutoResolutionFailed || !publisher.draft || publisher.status || report.Mergeable {
		t.Fatalf("failed regeneration should preserve the existing blocked draft PR: report=%#v draft=%t status=%t", report, publisher.draft, publisher.status)
	}
	if !strings.Contains(publisher.body, "Automatic generated-file resolution did not complete") {
		t.Fatalf("PR body omitted generator fallback details: %s", publisher.body)
	}
	for _, call := range *runner.calls {
		if strings.HasPrefix(call, "git push ") || strings.HasPrefix(call, "git commit ") {
			t.Fatalf("failed temporary merge mutated the published branch: %s", call)
		}
	}
}

func TestBuildPlanPreservesDivergedSyncBranch(t *testing.T) {
	syncSHA := "dddddddddddddddddddddddddddddddddddddddd"
	syncer := &Syncer{Git: plannerRunner(true, syncSHA, true)}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionConflictPreserveBranch {
		t.Fatalf("expected preserve branch action, got %q", plan.Action)
	}
	if plan.SyncBranchSHA != syncSHA {
		t.Fatalf("sync branch head changed in plan: %s", plan.SyncBranchSHA)
	}
}

func TestPublishConflictUpsertsDraftAndFailureStatus(t *testing.T) {
	publisher := &fakePublisher{}
	syncer := &Syncer{Git: plannerRunner(true, "", false), Publisher: publisher}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := syncer.Publish(context.Background(), testConfig(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if !publisher.draft {
		t.Fatal("conflict PR was not marked as draft")
	}
	if publisher.status || !strings.Contains(publisher.description, "conflicts") {
		t.Fatalf("conflict status should fail: status=%t description=%q", publisher.status, publisher.description)
	}
	if publisher.headSHA != plan.UpstreamSHA || report.PublishedHead != plan.UpstreamSHA {
		t.Fatalf("conflict PR head is not the pinned upstream commit: %s", publisher.headSHA)
	}
	if !strings.Contains(publisher.body, "shared.txt") {
		t.Fatalf("PR body omitted conflicting path: %s", publisher.body)
	}
}

func TestPublishPreservesDivergedSyncBranch(t *testing.T) {
	syncSHA := "dddddddddddddddddddddddddddddddddddddddd"
	runner := plannerRunner(true, syncSHA, true)
	publisher := &fakePublisher{}
	syncer := &Syncer{Git: runner, Publisher: publisher}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	report, err := syncer.Publish(context.Background(), testConfig(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.PublishedHead != syncSHA {
		t.Fatalf("diverged branch head changed: got %s, want %s", report.PublishedHead, syncSHA)
	}
	for _, call := range *runner.calls {
		if strings.HasPrefix(call, "git push ") {
			t.Fatalf("publisher attempted to rewrite preserved branch: %s", call)
		}
	}
	if !strings.Contains(publisher.body, "preserved") || publisher.status {
		t.Fatalf("preserved branch should report pending upstream changes: body=%q status=%t", publisher.body, publisher.status)
	}
}

func TestPublishCleanSyncUsesTemporaryWorktreeAndPostProcessor(t *testing.T) {
	runner := plannerRunner(false, "", false)
	publisher := &fakePublisher{}
	postProcessor := &fakePostProcessor{}
	syncer := &Syncer{Git: runner, Publisher: publisher, PostProcess: postProcessor}
	plan, err := syncer.BuildPlan(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Action != ActionCleanSync {
		t.Fatalf("expected clean action, got %q", plan.Action)
	}
	report, err := syncer.Publish(context.Background(), testConfig(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if report.PublishedHead != "ffffffffffffffffffffffffffffffffffffffff" {
		t.Fatalf("unexpected published head: %s", report.PublishedHead)
	}
	if postProcessor.runs != 1 {
		t.Fatalf("expected postprocessor once, got %d", postProcessor.runs)
	}
	if publisher.draft || !publisher.status {
		t.Fatalf("clean sync should create a mergeable non-draft PR: draft=%t status=%t", publisher.draft, publisher.status)
	}
	var sawWorktree, sawLease bool
	for _, call := range *runner.calls {
		sawWorktree = sawWorktree || strings.HasPrefix(call, "git worktree add --detach ")
		sawLease = sawLease || strings.Contains(call, "--force-with-lease=refs/heads/upstream-sync:")
	}
	if !sawWorktree || !sawLease {
		t.Fatalf("expected isolated worktree and lease-protected push; worktree=%t lease=%t", sawWorktree, sawLease)
	}
}

func TestCommandErrorClassifiesOnlyMergeTreeExitOneAsConflict(t *testing.T) {
	conflict := &CommandError{ExitCode: 1}
	if !isExitCode(conflict, 1) {
		t.Fatal("expected exit code 1 to be recognized")
	}
	if isExitCode(conflict, 128) {
		t.Fatal("exit code classification accepted the wrong code")
	}
	if !errors.As(conflict, new(*CommandError)) {
		t.Fatal("command error does not support errors.As")
	}
}

func TestGHPublisherUpdatesExistingPRAndConvertsItToDraft(t *testing.T) {
	var calls []string
	responses := []CommandResult{{Stdout: "42\n"}, {Stdout: "false\n"}, {}, {}}
	runner := fakeRunner{run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if len(responses) == 0 {
			return CommandResult{}, fmt.Errorf("unexpected extra command")
		}
		response := responses[0]
		responses = responses[1:]
		return response, nil
	}}
	publisher := GHPublisher{Runner: runner}
	plan := Plan{UpstreamSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := publisher.UpsertPR(context.Background(), testConfig(), plan, "conflict details", true); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(calls, "\n")
	if !strings.Contains(joined, "pr list") || !strings.Contains(joined, "pulls/42") || !strings.Contains(joined, "pr ready 42") || !strings.Contains(joined, "--undo") {
		t.Fatalf("publisher did not update and demote existing PR: %s", joined)
	}
	if strings.Contains(joined, "pr create") {
		t.Fatalf("publisher duplicated existing PR: %s", joined)
	}
}

func TestGHPublisherMarksExistingDraftPRReadyWhenSyncIsClean(t *testing.T) {
	var calls []string
	responses := []CommandResult{{Stdout: "42\n"}, {Stdout: "true\n"}, {}, {}}
	runner := fakeRunner{run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if len(responses) == 0 {
			return CommandResult{}, fmt.Errorf("unexpected extra command")
		}
		response := responses[0]
		responses = responses[1:]
		return response, nil
	}}
	publisher := GHPublisher{Runner: runner}
	plan := Plan{UpstreamSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := publisher.UpsertPR(context.Background(), testConfig(), plan, "clean sync", false); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(calls, "\n")
	if !strings.Contains(joined, "pr ready 42 --repo openshift/openshift-mcp-server") || strings.Contains(joined, "--undo") {
		t.Fatalf("publisher did not promote existing draft PR: %s", joined)
	}
	patchIndex := strings.Index(joined, "api --method PATCH repos/openshift/openshift-mcp-server/pulls/42")
	readyIndex := strings.Index(joined, "pr ready 42 --repo openshift/openshift-mcp-server")
	if patchIndex < 0 || readyIndex < 0 || patchIndex > readyIndex {
		t.Fatalf("publisher should update PR details before marking it ready: %s", joined)
	}
	if strings.Contains(joined, "pr create") {
		t.Fatalf("publisher duplicated existing PR: %s", joined)
	}
}

func TestGHPublisherSetsRequiredMergeabilityStatus(t *testing.T) {
	var call string
	runner := fakeRunner{run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		call = name + " " + strings.Join(args, " ")
		return CommandResult{}, nil
	}}
	if err := (GHPublisher{Runner: runner}).SetMergeability(context.Background(), testConfig(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", false, "conflicts remain"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(call, "statuses/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") || !strings.Contains(call, "state=failure") || !strings.Contains(call, MergeabilityContext) {
		t.Fatalf("publisher omitted failing mergeability status: %s", call)
	}
}

func TestGHPublisherCreatesDraftFromRemoteBranchWithoutCheckingItOut(t *testing.T) {
	var calls []string
	runner := fakeRunner{run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		if strings.HasPrefix(strings.Join(args, " "), "pr list ") {
			return CommandResult{Stdout: "null\n"}, nil
		}
		return CommandResult{}, nil
	}}
	plan := Plan{UpstreamSHA: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := (GHPublisher{Runner: runner}).UpsertPR(context.Background(), testConfig(), plan, "conflict report", true); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(calls, "\n")
	if !strings.Contains(joined, "api --method POST repos/openshift/openshift-mcp-server/pulls") ||
		!strings.Contains(joined, "head=upstream-sync") || !strings.Contains(joined, "draft=true") {
		t.Fatalf("draft PR was not created from the pushed remote head: %s", joined)
	}
	if strings.Contains(joined, "pr create") {
		t.Fatalf("publisher attempted local-branch-dependent PR creation: %s", joined)
	}
}

func TestGeneratedConflictAllowlistIsNarrow(t *testing.T) {
	for _, path := range []string{"README.md", "docs/configuration.md", "vendor/example/pkg.go", "pkg/mcp/testdata/tools.json"} {
		if _, ok := generatedRuleFor(path); !ok {
			t.Errorf("expected generated path %q to be allowlisted", path)
		}
	}
	for _, path := range []string{"pkg/api/tools.go", "go.mod", "pkg/mcp/testdata/fixture.yaml", "docs/design.md"} {
		if _, ok := generatedRuleFor(path); ok {
			t.Errorf("unexpected generated path allowlist for %q", path)
		}
	}
}

func TestGeneratedConflictPathParserAllowsOnlyConflictsInsideMarkers(t *testing.T) {
	markers := []markerPair{{start: "<!-- GENERATED-START -->", end: "<!-- GENERATED-END -->"}}
	content := []byte("prose before\n<!-- GENERATED-START -->\n<<<<<<< HEAD\nold output\n=======\nnew output\n>>>>>>> upstream\n<!-- GENERATED-END -->\nprose after\n")
	resolved, err := resolveGeneratedTextConflicts(content, markers)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(resolved), "<<<<<<<") || !strings.Contains(string(resolved), "new output") || !strings.Contains(string(resolved), "prose before") {
		t.Fatalf("generated conflict was not safely reduced: %s", resolved)
	}

	outside := []byte("<<<<<<< HEAD\nhandwritten local\n=======\nhandwritten upstream\n>>>>>>> upstream\n<!-- GENERATED-START -->\nold\n<!-- GENERATED-END -->\n")
	if _, err := resolveGeneratedTextConflicts(outside, markers); err == nil {
		t.Fatal("accepted a conflict outside the generated region")
	}
}

func TestResolveGeneratedREADMEConflictPreservesHandwrittenContent(t *testing.T) {
	worktree := t.TempDir()
	var content strings.Builder
	content.WriteString("handwritten introduction\n")
	for i, pair := range generatedDocMarkers {
		content.WriteString(pair.start + "\n")
		if i == 0 {
			content.WriteString("<<<<<<< HEAD\nold generated table\n=======\nupstream generated table\n>>>>>>> upstream\n")
		} else {
			content.WriteString("generated section\n")
		}
		content.WriteString(pair.end + "\n")
	}
	content.WriteString("handwritten conclusion\n")
	readme := filepath.Join(worktree, "README.md")
	if err := os.WriteFile(readme, []byte(content.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	var calls []string
	runner := fakeRunner{run: func(_ context.Context, _, name string, args ...string) (CommandResult, error) {
		calls = append(calls, name+" "+strings.Join(args, " "))
		return CommandResult{}, nil
	}}
	paths, err := resolveGeneratedConflicts(context.Background(), runner, worktree, []string{"README.md"})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "README.md" {
		t.Fatalf("unexpected regenerated paths: %#v", paths)
	}
	updated, err := os.ReadFile(readme)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(updated), "<<<<<<<") || !strings.Contains(string(updated), "upstream generated table") ||
		!strings.Contains(string(updated), "handwritten introduction") || !strings.Contains(string(updated), "handwritten conclusion") {
		t.Fatalf("generated conflict resolution damaged content: %s", updated)
	}
	if len(calls) != 1 || calls[0] != "git add --all -- README.md" {
		t.Fatalf("unexpected staging command in isolated fixture: %#v", calls)
	}
}

func TestGeneratedTextConflictMustBeInsideMarkedRegion(t *testing.T) {
	markers := []markerPair{{start: "<!-- GENERATED-START -->", end: "<!-- GENERATED-END -->"}}
	content := []byte("prose before\n<!-- GENERATED-START -->\n<<<<<<< HEAD\nold output\n=======\nnew output\n>>>>>>> upstream\n<!-- GENERATED-END -->\nprose after\n")
	resolved, err := resolveGeneratedTextConflicts(content, markers)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(resolved), "<<<<<<<") || !strings.Contains(string(resolved), "new output") || !strings.Contains(string(resolved), "prose before") {
		t.Fatalf("generated conflict was not safely reduced: %s", resolved)
	}

	outside := []byte("<<<<<<< HEAD\nhandwritten local\n=======\nhandwritten upstream\n>>>>>>> upstream\n<!-- GENERATED-START -->\nold\n<!-- GENERATED-END -->\n")
	if _, err := resolveGeneratedTextConflicts(outside, markers); err == nil {
		t.Fatal("accepted a conflict outside the generated region")
	}
}
