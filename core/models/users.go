package models

import (
	"time"
)

type UserDetails struct {
	ID           int64      `json:"id" db:"id"`
	FirstName    string     `json:"first_name" db:"first_name"`
	LastName     string     `json:"last_name" db:"last_name"`
	EmailAddress string     `json:"email_address" db:"email_address"`
	CreatedAt    *time.Time `json:"created_at,omitempty" db:"created_at"`
	DeletedAt    *time.Time `json:"deleted_at,omitempty" db:"deleted_at"`
	MergedAt     *time.Time `json:"merged_at,omitempty" db:"merged_at"`
	ParentUserId int64      `json:"parent_user_id,omitempty" db:"parent_user_id"`
}
