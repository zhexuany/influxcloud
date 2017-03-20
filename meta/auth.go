package meta

import (
	crand "crypto/rand"
	"crypto/sha256"
	"io"
)

const (
	// SaltBytes is the number of bytes used for salts
	SaltBytes = 32
)

// hashWithSalt returns a salted hash of password using salt
func hashWithSalt(salt []byte, password string) []byte {
	hasher := sha256.New()
	hasher.Write(salt)
	hasher.Write([]byte(password))
	return hasher.Sum(nil)
}

// saltedHash returns a salt and salted hash of password
func saltedHash(password string) (salt, hash []byte, err error) {
	salt = make([]byte, SaltBytes)
	if _, err := io.ReadFull(crand.Reader, salt); err != nil {
		return nil, nil, err
	}

	return salt, hashWithSalt(salt, password), nil
}
