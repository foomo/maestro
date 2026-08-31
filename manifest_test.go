package maestro_test

import (
	"strings"
	"testing"

	"github.com/foomo/maestro"
)

func validManifest() maestro.Manifest {
	return maestro.Manifest{
		Version: "abc",
		Files: []maestro.ManifestFile{
			{Name: "a.dat", Hash: "deadbeef", Size: 4},
			{Name: "sub/b.dat", Hash: "feedface", Size: 8},
		},
		TotalSize: 12,
	}
}

func TestManifestValidateOK(t *testing.T) {
	if err := validManifest().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestManifestRejectsPathTraversal(t *testing.T) {
	for _, bad := range []string{"../escape", "/abs", "x/../escape", "x/./y"} {
		m := validManifest()

		m.Files[0].Name = bad
		if err := m.Validate(); err == nil {
			t.Errorf("expected reject for %q", bad)
		}
	}
}

func TestManifestRejectsEmpty(t *testing.T) {
	m := maestro.Manifest{}
	if err := m.Validate(); err == nil {
		t.Error("empty manifest must fail validation")
	}
}

func TestManifestRejectsNegativeSize(t *testing.T) {
	m := validManifest()

	m.Files[0].Size = -1
	if err := m.Validate(); err == nil {
		t.Error("negative size must fail")
	}
}

func TestManifestRejectsDuplicateNames(t *testing.T) {
	m := validManifest()

	m.Files[1].Name = m.Files[0].Name
	if err := m.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate-name error, got %v", err)
	}
}
