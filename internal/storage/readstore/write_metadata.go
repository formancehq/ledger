package readstore

// isNullEncoded returns true if the encoded value starts with TypeTagNull.
func isNullEncoded(encodedValue []byte) bool {
	return len(encodedValue) > 0 && encodedValue[0] == TypeTagNull
}
