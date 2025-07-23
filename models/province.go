package models

import (
	"time"

	"gorm.io/gorm"
)

type Province struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name        string  `json:"name"`
	CountryUUID string  `json:"country_uuid" gorm:"type:varchar(255);not null"`
	Country     Country `gorm:"foreignKey:CountryUUID;references:UUID"`
	Signature   string  `json:"signature"`

	Users    []User    `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	Areas    []Area    `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	SubAreas []SubArea `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	Communes []Commune `gorm:"foreignKey:ProvinceUUID;references:UUID"`

	Brands    []Brand     `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	RoutePlan []RoutePlan `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	// PosForms  []PosForm   `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	// Pos       []Pos       `gorm:"foreignKey:ProvinceUUID;references:UUID"`

	TotalUsers int64 `json:"total_users"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`
}
