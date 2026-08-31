package maestro_test

import (
	"strings"
	"testing"

	"github.com/foomo/maestro"
)

func TestVersionStringType(t *testing.T) {
	var v maestro.Version = "abc"
	if !strings.HasPrefix(string(v), "ab") {
		t.Fail()
	}
}
