package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidToken = errors.New("invalid token")
	ErrExpiredToken = errors.New("token expired")
)

// Identity 是 token 內帶的使用者身分。國家來自帳號資料而不是裝置回報，避免有人偽造地區洗特定國家的排行。
type Identity struct {
	UserID  string `json:"sub"`
	Country string `json:"country"`
}

type claims struct {
	Identity
	IssuedAt  int64 `json:"iat"`
	ExpiresAt int64 `json:"exp"`
}

var header = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

type Signer struct {
	secret []byte
	now    func() time.Time
}

func NewSigner(secret []byte) *Signer {
	return &Signer{secret: secret, now: time.Now}
}

func (s *Signer) Issue(id Identity, ttl time.Duration) (string, error) {
	now := s.now()
	body, err := json.Marshal(claims{Identity: id, IssuedAt: now.Unix(), ExpiresAt: now.Add(ttl).Unix()})
	if err != nil {
		return "", fmt.Errorf("marshal claims: %w", err)
	}
	unsigned := header + "." + base64.RawURLEncoding.EncodeToString(body)
	return unsigned + "." + s.sign(unsigned), nil
}

func (s *Signer) Verify(token string) (Identity, error) {
	parts := strings.Split(token, ".")
	// header 必須和簽發時完全一致，擋掉 alg=none 這類降級攻擊。
	if len(parts) != 3 || parts[0] != header {
		return Identity{}, ErrInvalidToken
	}
	if !hmac.Equal([]byte(s.sign(parts[0]+"."+parts[1])), []byte(parts[2])) {
		return Identity{}, ErrInvalidToken
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	var c claims
	if err := json.Unmarshal(body, &c); err != nil || c.UserID == "" || len(c.Country) != 2 {
		return Identity{}, ErrInvalidToken
	}
	if s.now().Unix() >= c.ExpiresAt {
		return Identity{}, ErrExpiredToken
	}
	return c.Identity, nil
}

func (s *Signer) sign(unsigned string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(unsigned))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
