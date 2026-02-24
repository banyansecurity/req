package req

import (
	"net/http"
	"testing"
)

func TestMergingHeaders(t *testing.T) {
	actual := make(http.Header)

	actual.Add("user-agent", "my-actual-ua")
	actual.Add("authorization", "should-not-be-here")
	actual.Add("cookie", "should-also-not-be-here")

	merged := mergeHeaders(chromeHeaders, actual)

	if _, ok := merged["Authorization"]; ok {
		t.Fatal("authorization should not be here")
	}

	if _, ok := merged["Cookie"]; ok {
		t.Fatal("cookie should not be here")
	}
}
