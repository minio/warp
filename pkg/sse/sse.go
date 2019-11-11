package sse

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/probe"
	"github.com/minio/minio-go/v6/pkg/encrypt"
)

type ErrConflictSSE error

var errConflictSSE = func(sseServer, sseKeys string) *probe.Error {
	err := fmt.Errorf("SSE alias '%s' overlaps with SSE-C aliases '%s'", sseServer, sseKeys)
	return probe.NewError(ErrConflictSSE(err)).Untrace()
}

// parse and return encryption key pairs per alias.
func EncKeys(ctx *cli.Context) (map[string][]PrefixSSEPair, *probe.Error) {
	sseServer := os.Getenv("MC_ENCRYPT")
	if prefix := ctx.String("encrypt"); prefix != "" {
		sseServer = prefix
	}

	sseKeys := os.Getenv("MC_ENCRYPT_KEY")
	if keyPrefix := ctx.String("encrypt-key"); keyPrefix != "" {
		if sseServer != "" && strings.Contains(keyPrefix, sseServer) {
			return nil, errConflictSSE(sseServer, keyPrefix).Trace(ctx.Args()...)
		}
		sseKeys = keyPrefix
	}
	var err *probe.Error
	if sseKeys != "" {
		sseKeys, err = getDecodedKey(sseKeys)
		if err != nil {
			return nil, err.Trace(sseKeys)
		}
	}

	encKeyDB, err := parseAndValidateEncryptionKeys(sseKeys, sseServer)
	if err != nil {
		return nil, err.Trace(sseKeys)
	}

	return encKeyDB, nil
}

// decode if the key is encoded key and returns the key
func getDecodedKey(sseKeys string) (key string, err *probe.Error) {
	keyString := ""
	for i, sse := range strings.Split(sseKeys, ",") {
		if i > 0 {
			keyString = keyString + ","
		}
		sseString, err := parseKey(sse)
		if err != nil {
			return "", err
		}
		keyString = keyString + sseString
	}
	return keyString, nil
}

// Validate the key
func parseKey(sseKeys string) (sse string, err *probe.Error) {
	encryptString := strings.SplitN(sseKeys, "=", 2)
	secretValue := encryptString[1]
	if len(secretValue) == 32 {
		return sseKeys, nil
	}
	decodedString, e := base64.StdEncoding.DecodeString(secretValue)
	if e != nil || len(decodedString) != 32 {
		return "", probe.NewError(errors.New("Encryption key should be 32 bytes plain text key or 44 bytes base64 encoded key"))
	}
	return encryptString[0] + "=" + string(decodedString), nil
}

// parse and validate encryption keys entered on command line
func parseAndValidateEncryptionKeys(sseKeys string, sse string) (encMap map[string][]PrefixSSEPair, err *probe.Error) {
	encMap, err = parseEncryptionKeys(sseKeys)
	if err != nil {
		return nil, err
	}
	if sse != "" {
		for _, prefix := range strings.Split(sse, ",") {
			alias, _ := url2Alias(prefix)
			encMap[alias] = append(encMap[alias], PrefixSSEPair{
				Prefix: prefix,
				SSE:    encrypt.NewSSE(),
			})
		}
	}

	return encMap, nil
}

// parse list of comma separated alias/prefix=sse key values entered on command line and
// construct a map of alias to prefix and sse pairs.
func parseEncryptionKeys(sseKeys string) (encMap map[string][]PrefixSSEPair, err *probe.Error) {
	encMap = make(map[string][]PrefixSSEPair)
	if sseKeys == "" {
		return
	}
	prefix := ""
	index := 0 // start index of prefix
	vs := 0    // start index of sse-c key
	sseKeyLen := 32
	delim := 1
	k := len(sseKeys)
	for index < k {
		i := strings.Index(sseKeys[index:], "=")
		if i == -1 {
			return nil, probe.NewError(errors.New("SSE-C prefix should be of the form prefix1=key1,... "))
		}
		prefix = sseKeys[index : index+i]
		alias, _ := url2Alias(prefix)
		vs = i + 1 + index
		if vs+32 > k {
			return nil, probe.NewError(errors.New("SSE-C key should be 32 bytes long"))
		}
		if (vs+sseKeyLen < k) && sseKeys[vs+sseKeyLen] != ',' {
			return nil, probe.NewError(errors.New("SSE-C prefix=secret should be delimited by , and secret should be 32 bytes long"))
		}
		sseKey := sseKeys[vs : vs+sseKeyLen]
		if _, ok := encMap[alias]; !ok {
			encMap[alias] = make([]PrefixSSEPair, 0)
		}
		sse, e := encrypt.NewSSEC([]byte(sseKey))
		if e != nil {
			return nil, probe.NewError(e)
		}
		encMap[alias] = append(encMap[alias], PrefixSSEPair{
			Prefix: prefix,
			SSE:    sse,
		})
		// advance index sseKeyLen + delim bytes for the next key start
		index = vs + sseKeyLen + delim
	}

	// Sort encryption keys in descending order of prefix length
	for _, encKeys := range encMap {
		sort.Slice(encKeys, func(i, j int) bool {
			return len(encKeys[i].Prefix) > len(encKeys[j].Prefix)
		})
	}

	// Success.
	return encMap, nil
}

// struct representing object prefix and sse keys association.
type PrefixSSEPair struct {
	Prefix string
	SSE    encrypt.ServerSide
}

// get SSE Key if object prefix matches with given resource.
func GetSSE(resource string, encKeys []PrefixSSEPair) encrypt.ServerSide {
	for _, k := range encKeys {
		if strings.HasPrefix(resource, k.Prefix) {
			return k.SSE
		}
	}
	return nil
}
