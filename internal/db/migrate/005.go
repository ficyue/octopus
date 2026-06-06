package migrate

import (
	"fmt"

	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 5,
		Up:      migrateRelayLogColumnRename,
	})
}

// 005: copy data from old relay_logs column names to new axonhub-aligned names
// Old: input_tokens, output_tokens, cost, cached_tokens
// New: prompt_tokens, completion_tokens, total_cost, prompt_cached_tokens
func migrateRelayLogColumnRename(db *gorm.DB) error {
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

	// Only migrate if old columns exist
	if !hasColumn("relay_logs", "input_tokens") {
		return nil
	}

	// Copy data from old columns to new columns where new columns are 0 or null
	updates := []struct {
		oldCol string
		newCol string
	}{
		{"input_tokens", "prompt_tokens"},
		{"output_tokens", "completion_tokens"},
		{"cost", "total_cost"},
		{"cached_tokens", "prompt_cached_tokens"},
	}

	for _, u := range updates {
		if !hasColumn("relay_logs", u.newCol) {
			continue
		}
		sql := fmt.Sprintf(
			"UPDATE relay_logs SET %s = %s WHERE %s = 0 AND %s != 0",
			u.newCol, u.oldCol, u.newCol, u.oldCol,
		)
		if err := db.Exec(sql).Error; err != nil {
			// Log but don't fail - columns might not exist
			fmt.Printf("migrate 005: skip %s -> %s: %v\n", u.oldCol, u.newCol, err)
		}
	}

	return nil
}
