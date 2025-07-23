package models

import (
	"time"

	"gorm.io/gorm"
)

type Manager struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`
	
	Title       string  `gorm:"not null" json:"title"` // Example Head of Sales, Support, Manager, etc
	CountryUUID string  `json:"country_uuid" gorm:"type:varchar(255);not null"`
	Country     Country `gorm:"foreignKey:CountryUUID;references:UUID"`
	UserUUID    string  `json:"user_uuid" gorm:"type:varchar(255);not null"` // Corrected field name
	User        User    `gorm:"foreignKey:UserUUID;references:UUID"`
	Signature   string  `json:"signature"`
}
