package models

import (
	// "time"

	"time"

	"gorm.io/gorm"
)

type UserLogs struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name        string `gorm:"type:text;not null" json:"name"`
	UserUUID    string `json:"user_uuid" gorm:"type:varchar(255);not null"`
	User        User   `gorm:"foreignKey:UserUUID;references:UUID"`
	Action      string `gorm:"type:text;not null" json:"action"`
	Description string `gorm:"type:text;not null" json:"description"`
	Signature   string `json:"signature"`
}

