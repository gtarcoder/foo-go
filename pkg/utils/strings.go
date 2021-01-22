package utils

import (
	"math/rand"
)

func RandomString(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// generate n character string
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	for k, v := range buf {
		buf[k] = alphabet[v%byte(len(alphabet))]
	}

	return string(buf)
}
