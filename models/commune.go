package models

import (
	"time"

	"gorm.io/gorm"
)

type Commune struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name string `gorm:"not null" json:"name"`

	CountryUUID  string   `json:"country_uuid" gorm:"type:varchar(255);not null"`
	Country      Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	ProvinceUUID string   `json:"province_uuid" gorm:"type:varchar(255);not null"`
	Province     Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	AreaUUID     string   `json:"area_uuid" gorm:"type:varchar(255);not null"`
	Area         Area     `gorm:"foreignKey:AreaUUID;references:UUID"`
	SubAreaUUID  string   `json:"sub_area_uuid" gorm:"type:varchar(255);not null"`
	SubArea      SubArea  `gorm:"foreignKey:SubAreaUUID;references:UUID"`

	Signature string `json:"signature"`

	// RouthePlans []RoutePlan `gorm:"foreignKey:CommuneUUID;references:UUID"`
	// Pos         []Pos       `gorm:"foreignKey:CommuneUUID;references:UUID"`
	// PosForms    []PosForm   `gorm:"foreignKey:CommuneUUID;references:UUID"`

	// Users []User `gorm:"foreignKey:CommuneUUID;references:UUID"`

	TotalUsers int64 `json:"total_users"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`
}
