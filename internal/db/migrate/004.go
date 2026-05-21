package migrate

import (
	"fmt"

	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 4,
		Up:      addChannelKeyFailCount,
	})
}

// 004: add fail_count column to channel_keys
func addChannelKeyFailCount(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}

	dialect := db.Dialector.Name()

	hasColumn := func(table, column string) bool {
		switch dialect {
		case "sqlite":
			var name string
			db.Raw("SELECT name FROM pragma_table_info(?) WHERE name = ? LIMIT 1", table, column).Scan(&name)
			return name == column
		case "mysql":
			var count int64
			db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?", table, column).Scan(&count)
			return count > 0
		case "postgres":
			var count int64
			db.Raw("SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?", table, column).Scan(&count)
			return count > 0
		default:
			return db.Migrator().HasColumn(table, column)
		}
	}

	if hasColumn("channel_keys", "fail_count") {
		return nil
	}

	var sql string
	switch dialect {
	case "mysql":
		sql = "ALTER TABLE `channel_keys` ADD COLUMN `fail_count` INTEGER DEFAULT 0"
	case "postgres":
		sql = "ALTER TABLE channel_keys ADD COLUMN IF NOT EXISTS fail_count INTEGER DEFAULT 0"
	default:
		sql = "ALTER TABLE channel_keys ADD COLUMN fail_count INTEGER DEFAULT 0"
	}

	if err := db.Exec(sql).Error; err != nil {
		return fmt.Errorf("failed to add channel_keys.fail_count: %w", err)
	}

	return nil
}
