package models

import (
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type User struct {
	UUID string `gorm:"type:text;not null;unique;primaryKey" json:"uuid"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Fullname        string `gorm:"not null;default:''" json:"fullname"`
	Email           string `json:"email" gorm:"unique;default:''"`
	Title           string `json:"title" gorm:"default:''"`
	Phone           string `json:"phone" gorm:"not null;unique;default:''"` // Added unique constraint
	Password        string `json:"password" validate:"required" gorm:"default:''"`
	PasswordConfirm string `json:"password_confirm" gorm:"-"`

	Role       string `json:"role" gorm:"default:''"`
	Permission string `json:"permission" gorm:"default:''"`
	Image      string `json:"image" gorm:"default:''"`
	Status     bool   `json:"status" gorm:"default:true"`
	Signature  string `json:"signature" gorm:"default:''"`

	CountryUUID  string `json:"country_uuid" gorm:"type:varchar(255);not null;default:''"`
	ProvinceUUID string `json:"province_uuid" gorm:"type:varchar(255);not null;default:''"`
	AreaUUID     string `json:"area_uuid" gorm:"type:varchar(255);not null;default:''"`
	SubAreaUUID  string `json:"sub_area_uuid" gorm:"type:varchar(255);not null;default:''"`
	CommuneUUID  string `json:"commune_uuid" gorm:"type:varchar(255);not null;default:''"`

	Country  Country  `gorm:"foreignKey:CountryUUID;references:UUID"`
	Province Province `gorm:"foreignKey:ProvinceUUID;references:UUID"`
	Area     Area     `gorm:"foreignKey:AreaUUID;references:UUID"`
	SubArea  SubArea  `gorm:"foreignKey:SubAreaUUID;references:UUID"`
	Commune  Commune  `gorm:"foreignKey:CommuneUUID;references:UUID"`

	ManagerUUID string `json:"manager_uuid" gorm:"type:varchar(255)"`
	Manager     string `json:"manager" gorm:"default:''"`
	SupportUUID string `json:"support_uuid" gorm:"type:varchar(255)"`
	Support     string `json:"support" gorm:"default:''"`
	AsmUUID     string `json:"asm_uuid" gorm:"type:varchar(255)"`
	Asm         string `json:"asm" gorm:"default:''"`
	SupUUID     string `json:"sup_uuid" gorm:"type:varchar(255)"`
	Sup         string `json:"sup" gorm:"default:''"`
	DrUUID      string `json:"dr_uuid" gorm:"type:varchar(255)"`
	Dr          string `json:"dr" gorm:"default:''"`
	CycloUUID   string `json:"cyclo_uuid" gorm:"type:varchar(255)"`
	Cyclo       string `json:"cyclo" gorm:"default:''"`

	TotalSup   int64 `json:"total_sup"`
	TotalDr    int64 `json:"total_dr"`
	TotalCyclo int64 `json:"total_cyclo"`
	TotalPos   int64 `json:"total_pos"`
	Visites    int64 `json:"visites"`

	RoutePlan []RoutePlan `gorm:"foreignKey:UserUUID;references:UUID"`
	Pos       []Pos       `gorm:"foreignKey:UserUUID;references:UUID"`
	PosForms  []PosForm   `gorm:"foreignKey:UserUUID;references:UUID"`
	UserLogs  []UserLogs  `gorm:"foreignKey:UserUUID;references:UUID"`
}

type UserResponse struct {
	ID           uint
	UUID         string `json:"uuid"`
	Fullname     string `json:"fullname"`
	Email        string `json:"email"`
	Phone        string `json:"phone"`
	Title        string `json:"title"`
	Role         string `json:"role"`
	CountryUUID  string `json:"country_uuid"`
	Country      Country
	ProvinceUUID string `json:"province_uuid"`
	Province     Province
	AreaUUID     string `json:"area_uuid"`
	Area         Area
	SubAreaUUID  string `json:"sub_area_uuid"`
	SubArea      SubArea
	CommuneUUID  string `json:"commune_uuid"`
	Commune      Commune
	Permission   string `json:"permission"`
	Status       bool   `json:"status"`
	Signature    string `json:"signature"`
	CreatedAt    time.Time
	UpdatedAt    time.Time

	ManagerUUID string `json:"manager_uuid"`
	Manager     string `json:"manager" gorm:"default:''"`
	SupportUUID string `json:"support_uuid"`
	Support     string `json:"support" gorm:"default:''"`
	AsmUUID     string `json:"asm_uuid"`
	Asm         string `json:"asm" gorm:"default:''"`
	SupUUID     string `json:"sup_uuid"`
	Sup         string `json:"sup" gorm:"default:''"`
	DrUUID      string `json:"dr_uuid"`
	Dr          string `json:"dr" gorm:"default:''"`
	CycloUUID   string `json:"cyclo_uuid"`
	Cyclo       string `json:"cyclo" gorm:"default:''"`
}

type Login struct {
	// Email    string `json:"email" validate:"required,email"`
	// Phone    string `json:"phone" validate:"required"`
	Identifier string `json:"identifier" validate:"required"`
	Password   string `json:"password" validate:"required"`
}

func (u *User) SetPassword(p string) {
	hp, _ := bcrypt.GenerateFromPassword([]byte(p), 14)
	u.Password = string(hp)
}

func (u *User) ComparePassword(p string) error {
	err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(p))
	return err
}
