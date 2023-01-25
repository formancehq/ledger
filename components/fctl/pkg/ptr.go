package fctl

func BoolPointerToString(v *bool) string {
	if v == nil || !*v {
		return "No"
	}
	return "Yes"
}

func StringPointerToString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
