package models

import (
	"time"

	"gorm.io/gorm"
)

type RoutePlanItem struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	RoutePlanUUID string `json:"routeplan_uuid" gorm:"type:varchar(255);not null"`
	RoutePlan     RoutePlan `gorm:"foreignKey:RoutePlanUUID;references:UUID"`

	PosUUID string `json:"pos_uuid" gorm:"type:varchar(255);not null"`
	Pos     Pos    `gorm:"foreignKey:PosUUID;references:UUID"`

	Status bool `json:"status"`
}
