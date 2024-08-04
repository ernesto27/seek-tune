package utils

import (
	"errors"
	"os"
	"song-recognition/models"
)

type DBClient interface {
	StoreFingerprints(fingerprints map[uint32]models.Couple) error
	Close() error
	GetCouples(addresses []uint32) (map[uint32][]models.Couple, error)
	TotalSongs() (int, error)
	RegisterSong(songTitle, songArtist, ytID string) (uint32, error)
	GetSong(filterKey string, value interface{}) (s Song, songExists bool, e error)
	GetSongByID(songID uint32) (Song, bool, error)
	GetSongByYTID(ytID string) (Song, bool, error)
	GetSongByKey(key string) (Song, bool, error)
	DeleteSongByID(songID uint32) error
	DeleteStorage(name string) error
	GetStorageName() string
}

type Song struct {
	Title     string
	Artist    string
	YouTubeID string
}

const FILTER_KEYS = "_id | ytID | key"

// NewDbClient creates a new instance of DbClient
func NewDbClient() (DBClient, error) {
	storageType := os.Getenv("STORAGE_TYPE")

	if storageType == "" {
		storageType = "mongodb"
	}

	switch storageType {
	case "mongodb":
		return newMongoDB()
	case "sqlite":
		return newSQLiteDB()
	}

	return nil, errors.New("unsupported database")
}
