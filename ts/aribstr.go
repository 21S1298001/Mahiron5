package ts

import (
	"bytes"
	"io"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

// DecodeARIBString decodes an ARIB STD-B24 encoded byte sequence to a UTF-8 string.
//
// This is implemented incrementally: common character sets are supported first,
// and unknown characters are replaced with the Unicode replacement character.
func DecodeARIBString(b []byte) (string, error) {
	var out strings.Builder
	kanjiMode := true
	for i := 0; i < len(b); {
		switch b[i] {
		case 0x0e:
			kanjiMode = false
			i++
			continue
		case 0x0f:
			kanjiMode = true
			i++
			continue
		case 0x1b:
			if i+1 < len(b) && b[i+1] == 0x7e {
				kanjiMode = false
				i += 2
				continue
			}
			if i+2 < len(b) {
				i += 3
			} else {
				i = len(b)
			}
			continue
		}

		if !kanjiMode {
			out.WriteRune(decodeARIBAlnum(b[i]))
			i++
			continue
		}
		if i+1 >= len(b) {
			out.WriteRune(utf8.RuneError)
			break
		}
		if r, ok := decodeARIBExtraSymbol(b[i], b[i+1]); ok {
			out.WriteRune(r)
			i += 2
			continue
		}
		if s, err := decodeJISX0208Pair(b[i], b[i+1]); err == nil && s != "" {
			out.WriteString(s)
		} else {
			out.WriteRune(utf8.RuneError)
		}
		i += 2
	}
	return strings.ReplaceAll(out.String(), "\u3000\u3000", "\u3000"), nil
}

func decodeARIBAlnum(b byte) rune {
	if b == 0xfe {
		return '・'
	}
	b &= 0x7f
	if b == 0x20 || b == 0x21 {
		return '\u3000'
	}
	if b >= 0x30 && b <= 0x39 {
		return '０' + rune(b-0x30)
	}
	if b >= 0x41 && b <= 0x5a {
		return 'Ａ' + rune(b-0x41)
	}
	if b >= 0x61 && b <= 0x7a {
		return 'ａ' + rune(b-0x61)
	}
	if b >= 0x21 && b <= 0x7e {
		return 0xff01 + rune(b-0x21)
	}
	return utf8.RuneError
}

func decodeARIBExtraSymbol(first, second byte) (rune, bool) {
	if second == 0xfe && first >= 0x30 && first <= 0x39 {
		return '０' + rune(first-0x30), true
	}
	return utf8.RuneError, false
}

func decodeJISX0208Pair(first, second byte) (string, error) {
	input := []byte{0x1b, 0x24, 0x42, first, second, 0x1b, 0x28, 0x42}
	reader := transform.NewReader(bytes.NewReader(input), japanese.ISO2022JP.NewDecoder())
	out, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(out), nil
}
