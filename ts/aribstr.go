package ts

// DecodeARIBString decodes an ARIB STD-B24 encoded byte sequence to a UTF-8 string.
//
// This is implemented incrementally: common character sets are supported first,
// and unknown characters are replaced with the Unicode replacement character.
func DecodeARIBString(b []byte) (string, error) {
	// TODO: implement ARIB STD-B24 string decoding.
	return "", nil
}
