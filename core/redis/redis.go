package redis

import (
	"context"
	"encoding/json"
	"time"

	redis "github.com/redis/go-redis/v9"
	"github.com/viswals_backend_task/core/models"
	"github.com/viswals_backend_task/core/utils"
)

// in redis for suitability, data is stored as a key:value where value is in JSON format.

type Redis struct {
	client *redis.Client
	ttl    time.Duration
}

type options func(*Redis)error

func WithTTL(ttl string) options {
	return func(r *Redis) error {
		duration, err := utils.ParseTimeStamp(ttl)
		if err != nil {
			return err
		}
		r.ttl = duration
		return nil
	}
}
	


func New(connectionString string,opts ...options) (*Redis, error) {
	
	rc:=&Redis{}

	conf, err := redis.ParseURL(connectionString)
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(conf)
	status := client.Ping(context.Background())

	if status.Err() != nil {
		return nil, status.Err()
	}

	for _, opt := range opts{
		if err := opt(rc); err != nil {
			return nil, err
		}
	}
	rc.client = client

	return rc, nil
}

func (r *Redis) Get(ctx context.Context, key string) (*models.UserDetails, error) {
	out := r.client.Get(ctx, key)

	if out.Err() != nil {
		return nil, out.Err()
	}

	res, err := out.Result()
	if err != nil {
		return nil, err
	}

	var userDetails = new(models.UserDetails)

	err = json.Unmarshal([]byte(res), userDetails)
	if err != nil {
		return nil, err
	}

	return userDetails, nil
}

func (r *Redis) Set(ctx context.Context, key string, userDetails *models.UserDetails) error {

	// Validate and fix any invalid time.Time fields
	userDetails.CreatedAt = utils.FixInvalidTime(userDetails.CreatedAt)
	userDetails.DeletedAt = utils.FixInvalidTime(userDetails.DeletedAt)
	userDetails.MergedAt = utils.FixInvalidTime(userDetails.MergedAt)

	b, err := json.Marshal(userDetails)
	if err != nil {
		return err
	}

	out := r.client.Set(ctx, key, string(b), r.ttl)
	if out.Err() != nil {
		return out.Err()
	}

	return nil
}


func (r *Redis) Delete(ctx context.Context, key string) error {
	out := r.client.Del(ctx, key)
	if out.Err() != nil {
		return out.Err()
	}
	return nil
}
