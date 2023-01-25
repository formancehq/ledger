package fctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/formancehq/fctl/membershipclient"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/zitadel/oidc/pkg/client"
	"github.com/zitadel/oidc/pkg/client/rp"
	httphelper "github.com/zitadel/oidc/pkg/http"
	"github.com/zitadel/oidc/pkg/oidc"
	"golang.org/x/oauth2"
)

const AuthClient = "fctl"

type persistedProfile struct {
	MembershipURI       string        `json:"membershipURI"`
	Token               *oauth2.Token `json:"token"`
	DefaultOrganization string        `json:"defaultOrganization"`
}

type Profile struct {
	membershipURI       string
	token               *oauth2.Token
	defaultOrganization string
	config              *Config
}

func (p *Profile) ServicesBaseUrl(stack *membershipclient.Stack) *url.URL {
	baseUrl, err := url.Parse(stack.Uri)
	if err != nil {
		panic(err)
	}
	return baseUrl
}

func (p *Profile) ApiUrl(stack *membershipclient.Stack, service string) *url.URL {
	url := p.ServicesBaseUrl(stack)
	url.Path = "/api/" + service
	return url
}

func (p *Profile) UpdateToken(token *oauth2.Token) {
	p.token = token
	p.token.Expiry = p.token.Expiry.UTC()
}

func (p *Profile) SetMembershipURI(v string) {
	p.membershipURI = v
}

func (p *Profile) MarshalJSON() ([]byte, error) {
	return json.Marshal(persistedProfile{
		MembershipURI:       p.membershipURI,
		Token:               p.token,
		DefaultOrganization: p.defaultOrganization,
	})
}

func (p *Profile) UnmarshalJSON(data []byte) error {
	cfg := &persistedProfile{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return err
	}
	*p = Profile{
		membershipURI:       cfg.MembershipURI,
		token:               cfg.Token,
		defaultOrganization: cfg.DefaultOrganization,
	}
	return nil
}

func (p *Profile) GetMembershipURI() string {
	return p.membershipURI
}

func (p *Profile) GetDefaultOrganization() string {
	return p.defaultOrganization
}

func (p *Profile) GetToken(ctx context.Context, httpClient *http.Client) (*oauth2.Token, error) {
	if p.token == nil {
		return nil, errors.New("not authenticated")
	}
	if p.token != nil && p.token.Expiry.Before(time.Now()) {
		relyingParty, err := rp.NewRelyingPartyOIDC(p.membershipURI, AuthClient, "",
			"", []string{"openid", "email", "offline_access", "supertoken"}, rp.WithHTTPClient(httpClient))
		if err != nil {
			return nil, err
		}

		newToken, err := relyingParty.
			OAuthConfig().
			TokenSource(context.WithValue(ctx, oauth2.HTTPClient, httpClient), p.token).
			Token()
		if err != nil {
			return nil, err
		}

		p.UpdateToken(newToken)
		if err := p.config.Persist(); err != nil {
			return nil, err
		}
	}
	return p.token, nil
}

func (p *Profile) GetUserInfo(cmd *cobra.Command) (oidc.UserInfo, error) {

	relyingParty, err := GetAuthRelyingParty(cmd, p.GetMembershipURI())
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest(http.MethodGet, relyingParty.UserinfoEndpoint(), nil)
	if err != nil {
		return nil, err
	}

	token, err := p.GetToken(cmd.Context(), relyingParty.HttpClient())
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", fmt.Sprintf("%s %s", token.TokenType, token.AccessToken))
	userinfo := oidc.NewUserInfo()
	if err := httphelper.HttpRequest(relyingParty.HttpClient(), req, &userinfo); err != nil {
		return nil, err
	}
	return userinfo, nil
}

func (p *Profile) GetStackToken(ctx context.Context, httpClient *http.Client, stack *membershipclient.Stack) (string, error) {

	form := url.Values{
		"grant_type":         []string{string(oidc.GrantTypeTokenExchange)},
		"audience":           []string{fmt.Sprintf("stack://%s/%s", stack.OrganizationId, stack.Id)},
		"subject_token":      []string{p.token.AccessToken},
		"subject_token_type": []string{"urn:ietf:params:oauth:token-type:access_token"},
	}

	membershipDiscoveryConfiguration, err := client.Discover(p.membershipURI, httpClient)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, membershipDiscoveryConfiguration.TokenEndpoint,
		bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(AuthClient, "")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	ret, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}

	if ret.StatusCode != http.StatusOK {
		data, err := io.ReadAll(ret.Body)
		if err != nil {
			panic(err)
		}
		return "", errors.New(string(data))
	}

	securityToken := oauth2.Token{}
	if err := json.NewDecoder(ret.Body).Decode(&securityToken); err != nil {
		return "", err
	}

	apiUrl := p.ApiUrl(stack, "auth")
	form = url.Values{
		"grant_type": []string{"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  []string{securityToken.AccessToken},
		"scope":      []string{"openid email"},
	}

	stackDiscoveryConfiguration, err := client.Discover(apiUrl.String(), httpClient)
	if err != nil {
		return "", err
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, stackDiscoveryConfiguration.TokenEndpoint,
		bytes.NewBufferString(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth("fctl", "")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	ret, err = httpClient.Do(req)
	if err != nil {
		return "", err
	}

	if ret.StatusCode != http.StatusOK {
		data, err := io.ReadAll(ret.Body)
		if err != nil {
			panic(err)
		}
		return "", errors.New(string(data))
	}

	stackToken := oauth2.Token{}
	if err := json.NewDecoder(ret.Body).Decode(&stackToken); err != nil {
		return "", err
	}

	return stackToken.AccessToken, nil
}

func (p *Profile) SetDefaultOrganization(o string) {
	p.defaultOrganization = o
}

func (p *Profile) IsConnected() bool {
	return p.token != nil
}

type CurrentProfile Profile

func ListProfiles(cmd *cobra.Command, toComplete string) ([]string, error) {
	config, err := GetConfig(cmd)
	if err != nil {
		return []string{}, nil
	}

	ret := make([]string, 0)
	for p := range config.GetProfiles() {
		if strings.HasPrefix(p, toComplete) {
			ret = append(ret, p)
		}
	}
	sort.Strings(ret)
	return ret, nil
}
