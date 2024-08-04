package utils

import (
	"database/sql"
	"errors"
	"fmt"
	"song-recognition/models"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteDB struct {
	db *sql.DB
}

func newSQLiteDB() (*SQLiteDB, error) {
	db, err := sql.Open("sqlite3", "./song-recognition.db")
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %v", err)
	}

	sqlite := &SQLiteDB{db: db}
	if err := sqlite.InitTables(); err != nil {
		return nil, fmt.Errorf("failed to initialize tables: %v", err)
	}

	return sqlite, nil
}

func (sqlite *SQLiteDB) InitTables() error {
	createFingerprintsTable := `
    CREATE TABLE IF NOT EXISTS fingerprints (
        address INTEGER PRIMARY KEY,
        anchorTimeMs INTEGER,
        songID INTEGER
    );`

	createSongsTable := `
    CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY,
        key TEXT UNIQUE,
        ytID TEXT UNIQUE
    );`

	_, err := sqlite.db.Exec(createFingerprintsTable)
	if err != nil {
		return fmt.Errorf("failed to create fingerprints table: %v", err)
	}

	_, err = sqlite.db.Exec(createSongsTable)
	if err != nil {
		return fmt.Errorf("failed to create songs table: %v", err)
	}

	return nil
}

// Close closes the underlying SQLite database connection
func (sqlite *SQLiteDB) Close() error {
	if sqlite.db != nil {
		return sqlite.db.Close()
	}
	return nil
}

func (sqlite *SQLiteDB) StoreFingerprints(fingerprints map[uint32]models.Couple) error {
	tx, err := sqlite.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO fingerprints (address, anchorTimeMs, songID) VALUES (?, ?, ?) ON CONFLICT(address) DO UPDATE SET anchorTimeMs=excluded.anchorTimeMs, songID=excluded.songID")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for address, couple := range fingerprints {
		_, err := stmt.Exec(address, couple.AnchorTimeMs, couple.SongID)
		if err != nil {
			return fmt.Errorf("error upserting document: %s", err)
		}
	}

	return tx.Commit()
}

func (sqlite *SQLiteDB) GetCouples(addresses []uint32) (map[uint32][]models.Couple, error) {
	couples := make(map[uint32][]models.Couple)

	for _, address := range addresses {
		rows, err := sqlite.db.Query("SELECT anchorTimeMs, songID FROM fingerprints WHERE address = ?", address)
		if err != nil {
			return nil, fmt.Errorf("error retrieving document for address %d: %s", address, err)
		}
		defer rows.Close()

		var docCouples []models.Couple
		for rows.Next() {
			var couple models.Couple
			if err := rows.Scan(&couple.AnchorTimeMs, &couple.SongID); err != nil {
				return nil, err
			}
			docCouples = append(docCouples, couple)
		}
		couples[address] = docCouples
	}

	return couples, nil
}

func (sqlite *SQLiteDB) TotalSongs() (int, error) {
	var total int
	err := sqlite.db.QueryRow("SELECT COUNT(*) FROM songs").Scan(&total)
	if err != nil {
		return 0, err
	}

	return total, nil
}

func (sqlite *SQLiteDB) RegisterSong(songTitle, songArtist, ytID string) (uint32, error) {
	songID := GenerateUniqueID()
	key := GenerateSongKey(songTitle, songArtist)

	_, err := sqlite.db.Exec("INSERT INTO songs (id, key, ytID) VALUES (?, ?, ?)", songID, key, ytID)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return 0, fmt.Errorf("song with ytID or key already exists: %v", err)
		} else {
			return 0, fmt.Errorf("failed to register song: %v", err)
		}
	}

	return songID, nil
}

func (sqlite *SQLiteDB) GetSong(filterKey string, value interface{}) (s Song, songExists bool, e error) {
	if !strings.Contains(FILTER_KEYS, filterKey) {
		return Song{}, false, errors.New("invalid filter key")
	}

	var song Song
	query := fmt.Sprintf("SELECT key, ytID FROM songs WHERE %s = ?", filterKey)
	err := sqlite.db.QueryRow(query, value).Scan(&song.Title, &song.YouTubeID)
	if err != nil {
		if err == sql.ErrNoRows {
			return Song{}, false, nil
		}
		return Song{}, false, fmt.Errorf("failed to retrieve song: %v", err)
	}

	parts := strings.Split(song.Title, "---")
	if len(parts) != 2 {
		return Song{}, false, fmt.Errorf("invalid key format")
	}
	song.Title = parts[0]
	song.Artist = parts[1]

	return song, true, nil
}

func (sqlite *SQLiteDB) GetSongByID(songID uint32) (Song, bool, error) {
	return sqlite.GetSong("id", songID)
}

func (sqlite *SQLiteDB) GetSongByYTID(ytID string) (Song, bool, error) {
	return sqlite.GetSong("ytID", ytID)
}

func (sqlite *SQLiteDB) GetSongByKey(key string) (Song, bool, error) {
	return sqlite.GetSong("key", key)
}

func (sqlite *SQLiteDB) DeleteSongByID(songID uint32) error {
	_, err := sqlite.db.Exec("DELETE FROM songs WHERE id = ?", songID)
	if err != nil {
		return fmt.Errorf("failed to delete song: %v", err)
	}

	return nil
}

func (sqlite *SQLiteDB) DeleteStorage(name string) error {
	_, err := sqlite.db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	if err != nil {
		return fmt.Errorf("error deleting collection: %v", err)
	}
	return nil
}

func (sqlite *SQLiteDB) GetStorageName() string {
	return "SQLite"
}
