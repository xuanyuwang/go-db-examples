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
	"gorm.io/gorm/logger"
)

// struct for the example table
// Column A & B are primary keys
type Example struct {
	A sql.NullInt32 `gorm:"primaryKey"`
	B sql.NullTime  `gorm:"primaryKey"`
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
		&SmallerABiggerB, &SmallerASmallerB, &SmallerANullB,
		&BiggerABiggerB, &BiggerASmallerB, &BiggerANullB,
		&NullABiggerB, &NullASmallerB, &NullANullB,
	}

	columnA = OrderByColumn{SortExpresssion: "A", Direction: Asc, NullOption: Last}
	columnB = OrderByColumn{SortExpresssion: "B", Direction: Desc, NullOption: First}
)

type PaginationTest struct {
	suite.Suite
	postgres *embeddedpostgres.EmbeddedPostgres
	db       *gorm.DB
	cacheDir string
}

func TestPagination(t *testing.T) {
	suite.Run(t, &PaginationTest{})
}

func (t *PaginationTest) SetupSuite() {
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
func (t *PaginationTest) TearDownSuite() {
	err := t.postgres.Stop()
	t.Require().NoError(err)
	os.RemoveAll(t.cacheDir)
}

func (t *PaginationTest) insertAllRecords() {
	t.db.Exec(`
        CREATE TABLE IF NOT EXISTS examples (
            A INT NULL,
            B TIMESTAMP NULL,
            CONSTRAINT unique_ab UNIQUE (A, B)
        );`)
	t.db.Create(&AllRecords)
}

func (t *PaginationTest) cleanTable() {
	t.db.Exec("DROP TABLE IF EXISTS examples")
}

func (t *PaginationTest) SetupTest() {
	t.insertAllRecords()
}

func (t *PaginationTest) TearDownTest() {
	t.cleanTable()
}

func (t *PaginationTest) SetupSubTest() {
	t.insertAllRecords()
}

func (t *PaginationTest) TearDownSubTest() {
	t.cleanTable()
}

// common order by clause shared by all tests
func orderBy(db *gorm.DB) *gorm.DB {
	return db.Order("A ASC NULLS LAST, B DESC NULLS FIRST")
}

func (t *PaginationTest) TestAllRecords() {
	var records []*Example
	err := t.db.Model(&Example{}).Scopes(orderBy).Find(&records).Error
	t.Require().NoError(err)
	t.Require().Len(records, len(AllRecords))
	for i, r := range records {
		fmt.Printf("%v: %v\n", i, r.String())
	}
}

// Test the case where the last record of the last page are all NULLs
func (t *PaginationTest) TestLastRecordAllNulls() {
	condition := NextPage([]OrderByColumn{columnA, columnB}, []interface{}{
		convertValueToNil(NullANullB.A),
		convertValueToNil(NullANullB.B),
	})
	var records []*Example
	err := t.db.Model(&Example{}).Scopes(orderBy).Where(condition.SQL, condition.Values...).Find(&records).Error

	t.Require().NoError(err)
	t.Assert().Len(records, 2)
	for i, r := range records {
		fmt.Printf("%v: %v\n", i, r.String())
	}

}
