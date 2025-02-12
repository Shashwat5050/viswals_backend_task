package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	encryptions "github.com/viswals_backend_task/core/encryptions"
	"github.com/viswals_backend_task/core/models"
	"github.com/viswals_backend_task/core/postgres"
	"go.uber.org/zap"
)

// UserService provides operations for managing user data.
type UserService struct {
	dataStore  userStoreProvider       // Interface for database operations.
	cacheStore cacheStoreProvider      // Interface for caching operations.
	encryption *encryptions.Encryption // Encryption utility for secure data handling.
	logger     *zap.Logger             // Logger for structured logging.
}

// NewUserService initializes a new UserService instance.
func NewUserService(
	dataStore userStoreProvider,
	cacheStore cacheStoreProvider,
	encryption *encryptions.Encryption,
	logger *zap.Logger,
) *UserService {
	return &UserService{
		dataStore:  dataStore,
		cacheStore: cacheStore,
		encryption: encryption,
		logger:     logger,
	}
}

// GetUser retrieves a user's details by user ID.
// It first attempts to fetch the data from the cache; if not found, it fetches from the database.
func (us *UserService) GetUser(ctx context.Context, userID string) (*models.UserDetails, error) {
	// Attempt to fetch user details from the cache.
	user, err := us.cacheStore.Get(ctx, userID)
	if err != nil {
		us.logger.Warn("Error fetching user from cache", zap.String("user_id", userID), zap.Error(err))

		// If cache fetch fails, fallback to the database.
		user, err = us.dataStore.GetUserByID(ctx, userID)
		if err != nil {
			return nil, err
		}

		// Cache the retrieved user details for future use.
		if cacheErr := us.cacheStore.Set(ctx, userID, user); cacheErr != nil {
			us.logger.Warn("Error caching user details", zap.String("user_id", userID), zap.Error(cacheErr))
		}
	}

	// Decrypt the user's email address for secure display.
	decryptedEmail, err := us.encryption.Decrypt(user.EmailAddress)
	if err != nil {
		return nil, err
	}
	user.EmailAddress = decryptedEmail

	return user, nil
}

// GetAllUsers retrieves all users from the database and decrypts their email addresses.
func (us *UserService) GetAllUsers(ctx context.Context) ([]*models.UserDetails, error) {
	users, err := us.dataStore.GetAllUsers(ctx)
	if err != nil {
		return nil, err
	}

	// Decrypt email addresses for all retrieved users.
	for _, user := range users {
		decryptedEmail, err := us.encryption.Decrypt(user.EmailAddress)
		if err != nil {
			us.logger.Error("Error decrypting user email", zap.String("email", user.EmailAddress), zap.Error(err))
			return nil, err
		}
		user.EmailAddress = decryptedEmail
	}
	return users, nil
}

// DeleteUser deletes a user from the database and cache.
func (us *UserService) DeleteUser(ctx context.Context, userID string) error {
	// Delete the user from the database.
	if err := us.dataStore.DeleteUser(ctx, userID); err != nil {
		return err
	}

	// Remove the user from the cache.
	if err := us.cacheStore.Delete(ctx, userID); err != nil {
		us.logger.Warn("Error deleting user from cache", zap.String("user_id", userID))
		// Cache expiration will eventually remove the data.
	}
	return nil
}

// CreateUser adds a new user to the database and updates the cache.
func (us *UserService) CreateUser(ctx context.Context, user *models.UserDetails) error {
	// Encrypt the user's email address before saving.
	encryptedEmail, err := us.encryption.Encrypt(user.EmailAddress)
	if err != nil {
		return err
	}
	user.EmailAddress = encryptedEmail

	// Save the user in the database.
	if err := us.dataStore.CreateUser(ctx, user); err != nil {
		return err
	}

	// Cache the newly created user.
	if err := us.cacheStore.Set(ctx, fmt.Sprint(user.ID), user); err != nil {
		us.logger.Warn("Error caching user details", zap.Any("user", user), zap.Error(err))
	}
	return nil
}

// GetAllUsersSSE retrieves paginated user details for Server-Sent Events (SSE).
func (us *UserService) GetAllUsersSSE(ctx context.Context, limit, offset int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	var isLastBatch bool

	// Fetch paginated user data from the database.
	users, err := us.dataStore.ListUsers(ctx, limit, offset)
	if err != nil {
		if errors.Is(err, database.ErrNoData) {
			isLastBatch = true
		}
		return nil, err
	}

	// Decrypt email addresses for the fetched users.
	for _, user := range users {
		decryptedEmail, err := us.encryption.Decrypt(user.EmailAddress)
		if err != nil {
			us.logger.Error("Error decrypting user email", zap.String("email", user.EmailAddress), zap.Error(err))
			return nil, err
		}
		user.EmailAddress = decryptedEmail
	}

	// Serialize the users to JSON for SSE transmission.
	data, err := json.Marshal(users)
	if err != nil {
		return nil, err
	}

	if isLastBatch {
		return data, database.ErrNoData
	}
	return data, nil
}
