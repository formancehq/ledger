package sqlstorage

import (
	"context"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/oidc"
	"github.com/formancehq/auth/pkg/storage"
	"github.com/zitadel/oidc/pkg/op"
	"gorm.io/gorm"
)

func mapGormNotFoundError(err error) error {
	if err == gorm.ErrRecordNotFound {
		return storage.ErrNotFound
	}
	return err
}

var _ oidc.Storage = (*Storage)(nil)

type Storage struct {
	db *gorm.DB
}

func (s *Storage) FindTransientScopesByLabel(ctx context.Context, label string) ([]auth.Scope, error) {
	ret := make([]auth.Scope, 0)
	if err := s.db.
		WithContext(ctx).
		Model(ret).
		Where("label = ?", label).
		Association("TransientScopes").
		Find(&ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) FindTransientScopes(ctx context.Context, id string) ([]auth.Scope, error) {
	ret := make([]auth.Scope, 0)
	if err := s.db.
		WithContext(ctx).
		Model(ret).
		Where("id = ?", id).
		Association("TransientScopes").
		Find(&ret); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) CreateUser(ctx context.Context, user *auth.User) error {
	return s.db.WithContext(ctx).Create(user).Error
}

func (s *Storage) FindUserByEmail(ctx context.Context, email string) (*auth.User, error) {
	user := &auth.User{}
	return user, s.db.
		WithContext(ctx).
		First(user, "email = ?", email).
		Error
}

func New(db *gorm.DB) *Storage {
	return &Storage{
		db: db,
	}
}

// AuthRequestByID implements the op.Storage interface
// it will be called after the Login UI redirects back to the OIDC endpoint
func (s *Storage) AuthRequestByID(ctx context.Context, id string) (op.AuthRequest, error) {
	request := &auth.AuthRequest{}
	return request, s.db.
		WithContext(ctx).
		First(request, "id = ?", id).
		Error
}

func (s *Storage) AuthRequestByCode(ctx context.Context, code string) (op.AuthRequest, error) {
	request := &auth.AuthRequest{}
	return request, s.db.
		WithContext(ctx).
		First(request, "code = ?", code).
		Error
}

func (s *Storage) SaveAuthCode(ctx context.Context, id string, code string) error {
	return s.db.
		WithContext(ctx).
		Model(&auth.AuthRequest{}).
		Where("id = ?", id).
		Update("code", code).
		Error
}

func (s *Storage) SaveClient(ctx context.Context, client auth.Client) error {
	return s.db.
		WithContext(ctx).
		Save(client).
		Error
}

func (s *Storage) SaveAuthRequest(ctx context.Context, request auth.AuthRequest) error {
	return s.db.WithContext(ctx).Create(&request).Error
}

func (s *Storage) FindAuthRequest(ctx context.Context, id string) (*auth.AuthRequest, error) {
	ret := &auth.AuthRequest{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("id = ?", id).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) FindAuthRequestByCode(ctx context.Context, id string) (*auth.AuthRequest, error) {
	ret := &auth.AuthRequest{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("code = ?", id).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) UpdateAuthRequest(ctx context.Context, request auth.AuthRequest) error {
	return s.db.WithContext(ctx).Save(request).Error
}

func (s *Storage) UpdateAuthRequestCode(ctx context.Context, id string, code string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Model(&auth.AuthRequest{}).
		Where("id = ?", id).
		Update("code", code).
		Error)
}

func (s *Storage) DeleteAuthRequest(ctx context.Context, id string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Where("id = ?", id).
		Delete(&auth.AuthRequest{}).
		Error)
}

func (s *Storage) SaveRefreshToken(ctx context.Context, token auth.RefreshToken) error {
	return s.db.WithContext(ctx).Create(token).Error
}

func (s *Storage) FindRefreshToken(ctx context.Context, token string) (*auth.RefreshToken, error) {
	ret := &auth.RefreshToken{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("id = ?", token).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) DeleteRefreshToken(ctx context.Context, token string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Where("id = ?", token).
		Delete(&auth.RefreshToken{}).
		Error)
}

func (s *Storage) SaveAccessToken(ctx context.Context, token auth.AccessToken) error {
	return s.db.WithContext(ctx).Create(token).Error
}

func (s *Storage) FindAccessToken(ctx context.Context, token string) (*auth.AccessToken, error) {
	ret := &auth.AccessToken{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("id = ?", token).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) DeleteAccessToken(ctx context.Context, token string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Where("id = ?", token).
		Delete(&auth.AccessToken{}).
		Error)
}

func (s *Storage) DeleteAccessTokensForUserAndClient(ctx context.Context, userID string, clientID string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Where("user_id = ? AND application_id = ?", userID, clientID).
		Delete(&auth.AccessToken{}).
		Error)
}

func (s *Storage) DeleteAccessTokensByRefreshToken(ctx context.Context, token string) error {
	return mapGormNotFoundError(s.db.WithContext(ctx).
		Where("refresh_token_id = ?", token).
		Delete(&auth.AccessToken{}).
		Error)
}

func (s *Storage) FindUser(ctx context.Context, id string) (*auth.User, error) {
	ret := &auth.User{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("id = ?", id).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) FindClient(ctx context.Context, id string) (*auth.Client, error) {
	ret := &auth.Client{}
	if err := mapGormNotFoundError(s.db.WithContext(ctx).Where("id = ?", id).First(ret).Error); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *Storage) SaveUser(ctx context.Context, user auth.User) error {
	return s.db.WithContext(ctx).Create(user).Error
}

func (s *Storage) FindUserBySubject(ctx context.Context, subject string) (*auth.User, error) {
	user := &auth.User{}
	if err := s.db.WithContext(ctx).Where("subject = ?", subject).First(user).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	return user, nil
}
