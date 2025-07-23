package models

import (
	"time"

	"gorm.io/gorm"
)

type Pos struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Name      string `gorm:"not null" json:"name"` // Celui qui vend
	Shop      string `json:"shop"`                 // Nom du shop
	Postype   string `json:"postype"`              // Type de POS
	Gerant    string `json:"gerant"`               // name of the onwer of the pos
	Avenue    string `json:"avenue"`
	Quartier  string `json:"quartier"`
	Reference string `json:"reference"`
	Telephone string `json:"telephone"`
	Image     string `json:"image"`

	CountryUUID  string   `json:"country_uuid" gorm:"type:varchar(255);not null"`
	Country      Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	ProvinceUUID string   `json:"province_uuid" gorm:"type:varchar(255);not null"`
	Province     Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	AreaUUID     string   `json:"area_uuid" gorm:"type:varchar(255);not null"`
	Area         Area     `gorm:"foreignKey:AreaUUID;references:UUID"`
	SubAreaUUID  string   `json:"sub_area_uuid" gorm:"type:varchar(255);not null"`
	SubArea      SubArea  `gorm:"foreignKey:SubAreaUUID;references:UUID"`
	CommuneUUID  string   `json:"commune_uuid" gorm:"type:varchar(255);not null"`
	Commune      Commune  `gorm:"foreignKey:CommuneUUID;references:UUID"`

	UserUUID string `json:"user_uuid" gorm:"type:varchar(255);not null"`
	User     User   `gorm:"foreignKey:UserUUID;references:UUID"`

	AsmUUID   string `json:"asm_uuid" gorm:"type:varchar(255);not null"`
	Asm       string `json:"asm" gorm:"default:''"`
	SupUUID   string `json:"sup_uuid" gorm:"type:varchar(255);not null"`
	Sup       string `json:"sup" gorm:"default:''"`
	DrUUID    string `json:"dr_uuid" gorm:"type:varchar(255);not null"`
	Dr        string `json:"dr" gorm:"default:''"`
	CycloUUID string `json:"cyclo_uuid" gorm:"type:varchar(255);not null"`
	Cyclo     string `json:"cyclo" gorm:"default:''"`

	Status    bool   `json:"status"`
	Signature string `json:"signature"`

	Sync bool   `json:"sync"`

	// PosFormItems  []PosFormItems `gorm:"foreignKey:PosUUID;references:UUID"`
	PosForms      []PosForm      `gorm:"foreignKey:PosUUID;references:UUID"`
	PosEquipments []PosEquipment `gorm:"foreignKey:PosUUID;references:UUID"`
}
