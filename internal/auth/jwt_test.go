package auth

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestIssueAndVerify(t *testing.T) {
	s := NewSigner([]byte("secret"))
	token, err := s.Issue(Identity{UserID: "alice", Country: "TW"}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := s.Verify(token); err != nil || id != (Identity{UserID: "alice", Country: "TW"}) {
		t.Fatalf("verify = %+v, %v", id, err)
	}
}

func TestVerifyRejectsForgedAndExpiredTokens(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s := NewSigner([]byte("secret"))
	s.now = func() time.Time { return now }
	token, _ := s.Issue(Identity{UserID: "alice", Country: "TW"}, time.Minute)
	parts := strings.Split(token, ".")

	// 改成別的國家想拿到別的排行，或用 alg=none 跳過簽章，都要被擋下。
	forged := parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"alice","country":"US","exp":9999999999}`)) + "." + parts[2]
	none := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + parts[1] + "."
	other, _ := NewSigner([]byte("other")).Issue(Identity{UserID: "alice", Country: "TW"}, time.Minute)
	for name, tok := range map[string]string{"forged": forged, "alg none": none, "other secret": other, "garbage": "x"} {
		if _, err := s.Verify(tok); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("%s: err = %v", name, err)
		}
	}

	now = now.Add(2 * time.Minute)
	if _, err := s.Verify(token); !errors.Is(err, ErrExpiredToken) {
		t.Fatalf("expired: err = %v", err)
	}
}
