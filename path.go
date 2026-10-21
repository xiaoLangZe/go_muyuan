package download

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// pathKey normalises a resolved path for the "one active task per path" rule.
// Windows paths are case-insensitive.
func pathKey(p string) string {
	p = filepath.Clean(p)
	if runtime.GOOS == "windows" {
		p = strings.ToLower(p)
	}
	return p
}

func withinDir(base, target string) bool {
	if runtime.GOOS == "windows" {
		base = strings.ToLower(base)
		target = strings.ToLower(target)
	}
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if filepath.IsAbs(rel) {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

// resolveExistingPrefix resolves symlinks in the longest existing prefix of p
// and re-appends the parts that do not exist yet.
func resolveExistingPrefix(p string) (string, error) {
	cur := filepath.Clean(p)
	var suffix []string
	for {
		if resolved, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(append([]string{resolved}, suffix...)...), nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return "", fmt.Errorf("download: cannot resolve %s", p)
		}
		suffix = append([]string{filepath.Base(cur)}, suffix...)
		cur = parent
	}
}

// validateFileName rejects names that could escape the target directory or are
// illegal on the host platform.
func validateFileName(name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("%w: %q", ErrInvalidFileName, name)
	}
	if len(name) > 255 {
		return fmt.Errorf("%w: name is longer than 255 bytes", ErrInvalidFileName)
	}
	if strings.ContainsAny(name, `/\`) || strings.ContainsRune(name, 0) {
		return fmt.Errorf("%w: %q contains a path separator", ErrInvalidFileName, name)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: %q contains a control character", ErrInvalidFileName, name)
		}
	}
	if runtime.GOOS == "windows" {
		if strings.ContainsAny(name, `<>:"|?*`) {
			return fmt.Errorf("%w: %q contains a reserved character", ErrInvalidFileName, name)
		}
		if strings.HasSuffix(name, ".") || strings.HasSuffix(name, " ") {
			return fmt.Errorf("%w: %q has a trailing dot or space", ErrInvalidFileName, name)
		}
		base := strings.ToUpper(strings.TrimSuffix(name, filepath.Ext(name)))
		switch base {
		case "CON", "PRN", "AUX", "NUL",
			"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
			"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
			return fmt.Errorf("%w: %q is a reserved device name", ErrInvalidFileName, name)
		}
	}
	return nil
}

// resolveTarget turns (rootDir, saveDir, fileName) into an absolute path and
// enforces the configured containment rules. rootDir must be absolute when set.
func resolveTarget(rootDir, saveDir, fileName string, strict bool) (string, error) {
	if err := validateFileName(fileName); err != nil {
		return "", err
	}
	if strings.TrimSpace(saveDir) == "" {
		saveDir = "."
	}
	dir := saveDir
	if !filepath.IsAbs(dir) {
		if rootDir != "" {
			dir = filepath.Join(rootDir, dir)
		} else {
			abs, err := filepath.Abs(dir)
			if err != nil {
				return "", err
			}
			dir = abs
		}
	}
	dir = filepath.Clean(dir)
	if fi, err := os.Stat(dir); err == nil && !fi.IsDir() {
		return "", fmt.Errorf("%w: save directory %s is not a directory", ErrInvalidOption, dir)
	}
	if rootDir != "" && strict && !withinDir(rootDir, dir) {
		return "", fmt.Errorf("%w: %s is outside %s", ErrPathEscape, dir, rootDir)
	}
	target := filepath.Join(dir, fileName)
	if !withinDir(dir, target) {
		return "", fmt.Errorf("%w: %s", ErrPathEscape, target)
	}
	if rootDir != "" {
		resolvedDir, err := resolveExistingPrefix(dir)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrPathEscape, err)
		}
		resolvedRoot, err := resolveExistingPrefix(rootDir)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrPathEscape, err)
		}
		if strict && !withinDir(resolvedRoot, resolvedDir) {
			return "", fmt.Errorf("%w: %s resolves outside %s", ErrPathEscape, dir, rootDir)
		}
		if fi, err := os.Lstat(target); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%w: %s is a symlink", ErrPathEscape, target)
		}
	}
	return target, nil
}
