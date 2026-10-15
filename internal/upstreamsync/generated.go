package upstreamsync

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type markerPair struct {
	start string
	end   string
}

type generatedRule struct {
	wholeFile bool
	markers   []markerPair
}

var generatedDocMarkers = []markerPair{
	{start: "<!-- VALIDATED-PROJECTS-START -->", end: "<!-- VALIDATED-PROJECTS-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-START -->", end: "<!-- AVAILABLE-TOOLSETS-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-TOOLS-START -->", end: "<!-- AVAILABLE-TOOLSETS-TOOLS-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-PROMPTS-START -->", end: "<!-- AVAILABLE-TOOLSETS-PROMPTS-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-RESOURCES-START -->", end: "<!-- AVAILABLE-TOOLSETS-RESOURCES-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-RESOURCES-TEMPLATES-START -->", end: "<!-- AVAILABLE-TOOLSETS-RESOURCES-TEMPLATES-END -->"},
}

var generatedConfigurationMarkers = []markerPair{
	{start: "<!-- AVAILABLE-TOOLSETS-START -->", end: "<!-- AVAILABLE-TOOLSETS-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-RESOURCES-START -->", end: "<!-- AVAILABLE-TOOLSETS-RESOURCES-END -->"},
	{start: "<!-- AVAILABLE-TOOLSETS-RESOURCES-TEMPLATES-START -->", end: "<!-- AVAILABLE-TOOLSETS-RESOURCES-TEMPLATES-END -->"},
}

func generatedRuleFor(path string) (generatedRule, bool) {
	clean := filepath.ToSlash(filepath.Clean(path))
	switch clean {
	case "README.md":
		return generatedRule{markers: generatedDocMarkers}, true
	case "docs/configuration.md":
		return generatedRule{markers: generatedConfigurationMarkers}, true
	}
	if strings.HasPrefix(clean, "vendor/") {
		return generatedRule{wholeFile: true}, true
	}
	if filepath.Dir(clean) == "pkg/mcp/testdata" && filepath.Ext(clean) == ".json" {
		return generatedRule{wholeFile: true}, true
	}
	return generatedRule{}, false
}

func allConflictsGenerated(paths []string) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		if _, ok := generatedRuleFor(path); !ok {
			return false
		}
	}
	return true
}

// resolveGeneratedTextConflicts chooses the upstream side only for conflict
// blocks contained wholly inside a generated marker region. The generator
// replaces that region immediately afterward; handwritten content is kept.
func resolveGeneratedTextConflicts(content []byte, markers []markerPair) ([]byte, error) {
	lines := bytes.SplitAfter(content, []byte("\n"))
	regions, err := generatedRegions(lines, markers)
	if err != nil {
		return nil, err
	}
	var output bytes.Buffer
	resolved := 0
	for i := 0; i < len(lines); {
		line := strings.TrimSpace(string(lines[i]))
		if !strings.HasPrefix(line, "<<<<<<<") {
			output.Write(lines[i])
			i++
			continue
		}
		start := i
		separator := -1
		end := -1
		for j := start + 1; j < len(lines); j++ {
			marker := strings.TrimSpace(string(lines[j]))
			if strings.HasPrefix(marker, "<<<<<<<") {
				return nil, fmt.Errorf("nested conflict marker at line %d", j+1)
			}
			if separator < 0 && strings.HasPrefix(marker, "=======") {
				separator = j
				continue
			}
			if separator >= 0 && strings.HasPrefix(marker, ">>>>>>>") {
				end = j
				break
			}
		}
		if separator < 0 || end < 0 {
			return nil, fmt.Errorf("incomplete conflict marker block starting at line %d", start+1)
		}
		if !insideGeneratedRegion(start, end, regions) {
			return nil, fmt.Errorf("conflict marker block at lines %d-%d is outside generated regions", start+1, end+1)
		}
		// Keep the upstream side as temporary merge input; the declared
		// generator replaces this entire marker-bounded region next.
		for _, upstreamLine := range lines[separator+1 : end] {
			output.Write(upstreamLine)
		}
		resolved++
		i = end + 1
	}
	if resolved == 0 {
		return nil, fmt.Errorf("no generated conflict markers found")
	}
	return output.Bytes(), nil
}

type lineRange struct{ start, end int }

func generatedRegions(lines [][]byte, markers []markerPair) ([]lineRange, error) {
	var regions []lineRange
	for _, pair := range markers {
		start, end := -1, -1
		for i, line := range lines {
			text := strings.TrimSpace(string(line))
			if text == pair.start {
				if start >= 0 {
					return nil, fmt.Errorf("duplicate generated start marker %q", pair.start)
				}
				start = i
			}
			if text == pair.end {
				if end >= 0 {
					return nil, fmt.Errorf("duplicate generated end marker %q", pair.end)
				}
				end = i
			}
		}
		if start < 0 || end <= start {
			return nil, fmt.Errorf("generated marker pair %q / %q is missing or reversed", pair.start, pair.end)
		}
		regions = append(regions, lineRange{start: start + 1, end: end - 1})
	}
	return regions, nil
}

func insideGeneratedRegion(start, end int, regions []lineRange) bool {
	for _, region := range regions {
		if start >= region.start && end <= region.end {
			return true
		}
	}
	return false
}

func unmergedPaths(ctx context.Context, runner CommandRunner, worktree string) ([]string, error) {
	result, err := runChecked(ctx, runner, worktree, "git", "diff", "--name-only", "--diff-filter=U", "-z")
	if err != nil {
		return nil, fmt.Errorf("list unmerged paths: %w", err)
	}
	var paths []string
	for _, path := range strings.Split(result.Stdout, "\x00") {
		if path == "" {
			continue
		}
		if !filepath.IsLocal(path) {
			return nil, fmt.Errorf("Git returned non-local conflict path %q", path)
		}
		paths = append(paths, filepath.ToSlash(filepath.Clean(path)))
	}
	return union(paths), nil
}

func resolveGeneratedConflict(ctx context.Context, runner CommandRunner, worktree, path string) error {
	rule, ok := generatedRuleFor(path)
	if !ok {
		return fmt.Errorf("conflict path %q is not allowlisted as generated", path)
	}
	if rule.wholeFile {
		if _, err := runChecked(ctx, runner, worktree, "git", "checkout", "--theirs", "--", path); err != nil {
			return fmt.Errorf("select temporary upstream version of generated path %q: %w", path, err)
		}
	} else {
		localPath := filepath.Join(worktree, filepath.FromSlash(path))
		info, err := os.Stat(localPath)
		if err != nil {
			return fmt.Errorf("stat conflicted generated file %q: %w", path, err)
		}
		contents, err := os.ReadFile(localPath)
		if err != nil {
			return fmt.Errorf("read conflicted generated file %q: %w", path, err)
		}
		resolved, err := resolveGeneratedTextConflicts(contents, rule.markers)
		if err != nil {
			return fmt.Errorf("resolve generated regions in %q: %w", path, err)
		}
		if err := os.WriteFile(localPath, resolved, info.Mode().Perm()); err != nil {
			return fmt.Errorf("write temporary generated conflict resolution for %q: %w", path, err)
		}
	}
	if _, err := runChecked(ctx, runner, worktree, "git", "add", "--all", "--", path); err != nil {
		return fmt.Errorf("stage temporary generated conflict resolution for %q: %w", path, err)
	}
	return nil
}

func resolveGeneratedConflicts(ctx context.Context, runner CommandRunner, worktree string, paths []string) ([]string, error) {
	var wholeFiles, markedFiles []string
	for _, path := range paths {
		rule, ok := generatedRuleFor(path)
		if !ok {
			return nil, fmt.Errorf("conflict path %q is not allowlisted as generated", path)
		}
		if rule.wholeFile {
			wholeFiles = append(wholeFiles, path)
		} else {
			markedFiles = append(markedFiles, path)
		}
	}
	sort.Strings(wholeFiles)
	sort.Strings(markedFiles)
	for start := 0; start < len(wholeFiles); start += 100 {
		end := start + 100
		if end > len(wholeFiles) {
			end = len(wholeFiles)
		}
		batch := wholeFiles[start:end]
		checkoutArgs := append([]string{"checkout", "--theirs", "--"}, batch...)
		if _, err := runChecked(ctx, runner, worktree, "git", checkoutArgs...); err != nil {
			return nil, fmt.Errorf("select temporary upstream versions of generated files: %w", err)
		}
		addArgs := append([]string{"add", "--all", "--"}, batch...)
		if _, err := runChecked(ctx, runner, worktree, "git", addArgs...); err != nil {
			return nil, fmt.Errorf("stage temporary generated file resolutions: %w", err)
		}
	}
	for _, path := range markedFiles {
		if err := resolveGeneratedConflict(ctx, runner, worktree, path); err != nil {
			return nil, err
		}
	}
	return union(paths), nil
}
