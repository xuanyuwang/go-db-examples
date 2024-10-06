package pagination

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// struct for the example table
// Column A & B are primary keys
type Example struct {
	A sql.NullInt32 `gorm:"column:a"`
	B sql.NullTime  `gorm:"column:b"`
}

func (e *Example) String() string {
	var a, b interface{}
	if e.A.Valid {
		a = e.A.Int32
	} else {
		a = "NULL"
	}
	if e.B.Valid {
		b = e.B.Time.Format(time.DateOnly)
	} else {
		b = "NULL"
	}
	return fmt.Sprintf("A: %v,\tB: %v", a, b)
}

func convertValueToNil(v interface{}) interface{} {
	switch v.(type) {
	case sql.NullInt32:
		x := v.(sql.NullInt32)
		if x.Valid {
			return x.Int32
		} else {
			return nil
		}
	case sql.NullTime:
		x := v.(sql.NullTime)
		if x.Valid {
			return x.Time
		} else {
			return nil
		}
	default:
		return nil
	}
}

var (
	SmallerDate, _  = time.Parse(time.DateOnly, "2020-01-31")
	SmallerNullTime = sql.NullTime{Time: SmallerDate, Valid: true}
	BiggerDate, _   = time.Parse(time.DateOnly, "2020-02-01")
	BiggerNullTime  = sql.NullTime{Time: BiggerDate, Valid: true}
	SmallerNullInt  = sql.NullInt32{Int32: 20, Valid: true}
	BiggerNullInt   = sql.NullInt32{Int32: 21, Valid: true}

	SmallerABiggerB  = Example{A: SmallerNullInt, B: BiggerNullTime}
	SmallerASmallerB = Example{A: SmallerNullInt, B: SmallerNullTime}
	SmallerANullB    = Example{A: SmallerNullInt}

	BiggerABiggerB  = Example{A: BiggerNullInt, B: BiggerNullTime}
	BiggerASmallerB = Example{A: BiggerNullInt, B: SmallerNullTime}
	BiggerANullB    = Example{A: BiggerNullInt}

	NullABiggerB  = Example{B: BiggerNullTime}
	NullASmallerB = Example{B: SmallerNullTime}
	NullANullB    = Example{}

	AllRecords = []*Example{
		&SmallerANullB, &SmallerABiggerB, &SmallerASmallerB,
		&BiggerANullB, &BiggerABiggerB, &BiggerASmallerB,
		&NullANullB, &NullABiggerB, &NullASmallerB,
	}
)

type NextPageConditonTest struct {
	suite.Suite
	postgres *embeddedpostgres.EmbeddedPostgres
	db       *gorm.DB
	cacheDir string
}

func TestNextPageCondition(t *testing.T) {
	suite.Run(t, &NextPageConditonTest{})
}

func (t *NextPageConditonTest) SetupSuite() {
	cachePath := fmt.Sprintf("embedded-postgres-go-%s", uuid.NewString())
	cacheDir, err := os.MkdirTemp("", cachePath)
	t.Require().NoError(err)
	t.cacheDir = cacheDir
	t.postgres = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().CachePath(t.cacheDir))
	err = t.postgres.Start()
	t.Require().NoError(err)
	dsn := "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable"
	t.db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	t.Require().NoError(err)
}

// Stop the database
func (t *NextPageConditonTest) TearDownSuite() {
	err := t.postgres.Stop()
	t.Require().NoError(err)
	os.RemoveAll(t.cacheDir)
}

func (t *NextPageConditonTest) insertAllRecords() {
	t.db.Exec(`
        CREATE TABLE IF NOT EXISTS examples (
            A INT NULL,
            B TIMESTAMP NULL,
            CONSTRAINT unique_ab UNIQUE (A, B)
        );`)
	t.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&AllRecords)
}

func (t *NextPageConditonTest) cleanTable() {
	t.db.Exec("DROP TABLE IF EXISTS examples")
}

func (t *NextPageConditonTest) SetupTest() {
	t.cleanTable()
	t.insertAllRecords()
}

func (t *NextPageConditonTest) TearDownTest() {
	t.cleanTable()
}

func (t *NextPageConditonTest) SetupSubTest() {
	t.cleanTable()
	t.insertAllRecords()
}

func (t *NextPageConditonTest) TearDownSubTest() {
	t.cleanTable()
}

// Test the case where the last record of the last page are all NULLs
func (t *NextPageConditonTest) TestNextPage() {
	t.Run("ORDER BY A ASC NULLS LAST, B DESC NULLS FIRST", func() {
		columnA := OrderByColumn{SortExpresssion: "A", Direction: Asc, NullOption: Last}
		columnB := OrderByColumn{SortExpresssion: "B", Direction: Desc, NullOption: First}
		orderByColumns := []OrderByColumn{columnA, columnB}
		// in sorted order of "A ASC NULLS LAST, B DESC, NULLS FIRST"
		AllSortedRecords := []*Example{
			&SmallerANullB, &SmallerABiggerB, &SmallerASmallerB,
			&BiggerANullB, &BiggerABiggerB, &BiggerASmallerB,
			&NullANullB, &NullABiggerB, &NullASmallerB,
		}
		t.Run("All records", func() {
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Find(&records).Error
			t.Require().NoError(err)
			t.Require().Len(records, len(AllSortedRecords))
			for i, r := range records {
				fmt.Printf("%v: %v\n", i, r.String())
			}
		})

		t.Run("When A is null and B is null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(NullANullB.A),
				convertValueToNil(NullANullB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, 2)
			t.Assert().ElementsMatch(records, AllSortedRecords[7:])
		})

		t.Run("When A is null and B is not null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(NullABiggerB.A),
				convertValueToNil(NullABiggerB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, 1)
			t.Assert().ElementsMatch(records, AllSortedRecords[8:])
		})

		t.Run("When A is not null and B is null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(SmallerANullB.A),
				convertValueToNil(SmallerANullB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, 8)
			t.Assert().ElementsMatch(records, AllSortedRecords[1:])
		})

		t.Run("When A is not null and B is not null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(SmallerABiggerB.A),
				convertValueToNil(SmallerABiggerB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, 7)
			t.Assert().ElementsMatch(records, AllSortedRecords[2:])
		})
	})

	t.Run("ORDER BY A DESC NULLS FIRST, B ASC NULLS LAST", func() {
		columnA := OrderByColumn{SortExpresssion: "A", Direction: Desc, NullOption: First}
		columnB := OrderByColumn{SortExpresssion: "B", Direction: Asc, NullOption: Last}
		orderByColumns := []OrderByColumn{columnA, columnB}
		// in sorted order of "A ASC NULLS LAST, B DESC, NULLS FIRST"
		AllSortedRecords := []*Example{
			&NullASmallerB, &NullABiggerB, &NullANullB,
			&BiggerASmallerB, &BiggerABiggerB, &BiggerANullB,
			&SmallerASmallerB, &SmallerABiggerB, &SmallerANullB,
		}
		t.Run("All records", func() {
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Find(&records).Error
			t.Require().NoError(err)
			t.Require().Len(records, len(AllSortedRecords))
			t.Require().ElementsMatch(records, AllSortedRecords)
		})

		t.Run("When A is null and B is null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(NullANullB.A),
				convertValueToNil(NullANullB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, len(AllSortedRecords[3:]))
			t.Assert().ElementsMatch(records, AllSortedRecords[3:])
		})

		t.Run("When A is null and B is not null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(NullASmallerB.A),
				convertValueToNil(NullASmallerB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, len(AllSortedRecords[1:]))
			t.Assert().ElementsMatch(records, AllSortedRecords[1:])
		})

		t.Run("When A is not null and B is null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(BiggerANullB.A),
				convertValueToNil(BiggerANullB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, len(AllSortedRecords[6:]))
			t.Assert().ElementsMatch(records, AllSortedRecords[6:])
		})

		t.Run("When A is not null and B is not null in last record", func() {
			condition := NextPageConditon([]OrderByColumn{columnA, columnB}, []interface{}{
				convertValueToNil(BiggerASmallerB.A),
				convertValueToNil(BiggerASmallerB.B),
			})
			var records []*Example
			err := t.db.Model(&Example{}).Scopes(orderByScope(orderByColumns...)).Where(condition.SQL, condition.Values...).Find(&records).Error

			t.Require().NoError(err)
			t.Assert().Len(records, len(AllSortedRecords[4:]))
			t.Assert().ElementsMatch(records, AllSortedRecords[4:])
		})
	})
}
