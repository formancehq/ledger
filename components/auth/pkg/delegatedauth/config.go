package delegatedauth

type Config struct {
	Issuer       string
	ClientID     string
	ClientSecret string
	RedirectURL  string
}
