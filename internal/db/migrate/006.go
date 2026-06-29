package migrate

import (
	"fmt"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 6,
		Up:      migrateAddKeyMode,
	})
}

// 006: Add key_mode column to channels table
func migrateAddKeyMode(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	
	// Check if column exists
	var count int
	db.Raw("SELECT COUNT(*) FROM pragma_table_info('channels') WHERE name='key_mode'").Scan(&count)
	if count > 0 {
		return nil
	}
	
	sql := "ALTER TABLE `channels` ADD COLUMN `key_mode` INTEGER DEFAULT 0"
	if err := db.Exec(sql).Error; err != nil {
		return err
	}
	
	return nil
}
