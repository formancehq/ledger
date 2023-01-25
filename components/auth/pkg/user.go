package auth

type User struct {
	ID      string `json:"id" gorm:"primarykey"`
	Subject string `json:"subject" gorm:"unique"`
	Email   string `json:"email"`
}
