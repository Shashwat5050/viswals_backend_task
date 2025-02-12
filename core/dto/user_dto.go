package dto

// RawUserData used for storing and retrieving user data to and from the CSV and RabbitMQ queue.
// It does not represent the database model, It the data that is being extracted from the CSV file.
type RawUserData struct {
	Id           int64
	FirstName    string
	LastName     string
	Email        string
	ParentUserId int64
	CreatedAt    int64
	DeletedAt    int64
	MergedAt     int64
}