package webhooks

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Config struct {
	bun.BaseModel `bun:"table:configs"`

	ConfigUser

	ID        string    `json:"id" bun:",pk"`
	Active    bool      `json:"active"`
	CreatedAt time.Time `json:"createdAt" bun:"created_at,nullzero,notnull,default:current_timestamp"`
	UpdatedAt time.Time `json:"updatedAt" bun:"updated_at,nullzero,notnull,default:current_timestamp"`
}

type ConfigUser struct {
	Endpoint   string   `json:"endpoint"`
	Secret     string   `json:"secret"`
	EventTypes []string `json:"eventTypes" bun:"event_types,array"`
}

var _ bun.AfterCreateTableHook = (*Config)(nil)

func (*Config) AfterCreateTable(ctx context.Context, q *bun.CreateTableQuery) error {
	if _, err := q.DB().NewCreateIndex().IfNotExists().
		Model((*Config)(nil)).
		Index("configs_idx").
		Column("event_types").
		Exec(ctx); err != nil {
		return errors.Wrap(err, "creating configs index")
	}

	return nil
}

func NewConfig(cfgUser ConfigUser) Config {
	return Config{
		ConfigUser: cfgUser,
		ID:         uuid.NewString(),
		Active:     true,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
}

var (
	ErrInvalidEndpoint   = errors.New("endpoint should be a valid url")
	ErrInvalidEventTypes = errors.New("eventTypes should be filled")
	ErrInvalidSecret     = errors.New("decoded secret should be of size 24")
)

func (c *ConfigUser) Validate() error {
	if u, err := url.Parse(c.Endpoint); err != nil || len(u.String()) == 0 {
		return ErrInvalidEndpoint
	}

	if c.Secret == "" {
		c.Secret = NewSecret()
	} else {
		if decoded, err := base64.StdEncoding.DecodeString(c.Secret); err != nil {
			return fmt.Errorf("secret should be base64 encoded: %w", err)
		} else if len(decoded) != 24 {
			return ErrInvalidSecret
		}
	}

	if len(c.EventTypes) == 0 {
		return ErrInvalidEventTypes
	}

	for i, t := range c.EventTypes {
		if len(t) == 0 {
			return ErrInvalidEventTypes
		}
		c.EventTypes[i] = strings.ToLower(t)
	}

	return nil
}
