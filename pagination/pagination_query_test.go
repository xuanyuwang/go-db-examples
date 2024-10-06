package pagination

import (
	"context"
	"fmt"
	"os"
	"testing"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

type PaginationQueryTest struct {
	suite.Suite
	postgres *embeddedpostgres.EmbeddedPostgres
	db       *gorm.DB
	cacheDir string
}

func TestPaginationQuery(t *testing.T) {
	suite.Run(t, &PaginationQueryTest{})
}

func (t *PaginationQueryTest) SetupSuite() {
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
func (t *PaginationQueryTest) TearDownSuite() {
	err := t.postgres.Stop()
	t.Require().NoError(err)
	os.RemoveAll(t.cacheDir)
}

func (t *PaginationQueryTest) insertAllRecords() {
	t.db.Exec(`
        CREATE TABLE IF NOT EXISTS examples (
            A INT NULL,
            B TIMESTAMP NULL,
            CONSTRAINT unique_ab UNIQUE (A, B)
        );`)
	t.db.Clauses(clause.OnConflict{DoNothing: true}).Create(&AllRecords)
}

func (t *PaginationQueryTest) cleanTable() {
	t.db.Exec("DROP TABLE IF EXISTS examples")
}

func (t *PaginationQueryTest) SetupTest() {
	t.cleanTable()
	t.insertAllRecords()
}

func (t *PaginationQueryTest) TearDownTest() {
	t.cleanTable()
}

func (t *PaginationQueryTest) SetupSubTest() {
	t.cleanTable()
	t.insertAllRecords()
}

func (t *PaginationQueryTest) TearDownSubTest() {
	t.cleanTable()
}

func (t *PaginationQueryTest) TestPagination() {
	columnA := OrderByColumn{SortExpresssion: "A", Direction: Asc, NullOption: Last}
	columnB := OrderByColumn{SortExpresssion: "B", Direction: Desc, NullOption: First}
	orderByColumns := []OrderByColumn{columnA, columnB}
	// in sorted order of "A ASC NULLS LAST, B DESC, NULLS FIRST"
	AllSortedRecords := []*Example{
		&SmallerANullB, &SmallerABiggerB, &SmallerASmallerB,
		&BiggerANullB, &BiggerABiggerB, &BiggerASmallerB,
		&NullANullB, &NullABiggerB, &NullASmallerB,
	}
	ctx := context.Background()

	t.Run("Fetch all records at once", func() {
		records := []*Example{}
		pageSize := 0
		nextPageToken := ""
		nextPageToken, err := PaginatedQuery(
			ctx, &records, t.db,
			func(d *gorm.DB) *gorm.DB { return d.Model(&Example{}) },
			pageSize, nextPageToken,
			orderByColumns,
		)
		t.Require().NoError(err)
		t.Require().Empty(nextPageToken)
		t.Require().ElementsMatch(records, AllSortedRecords)
	})

	t.Run("Fetch with pageSize = 4", func() {
		records := []*Example{}
		pageSize := 4
		nextPageToken := ""
		nextPageToken, err := PaginatedQuery(
			ctx, &records, t.db,
			func(d *gorm.DB) *gorm.DB { return d.Model(&Example{}) },
			pageSize, nextPageToken,
			orderByColumns,
		)
		t.Require().NoError(err)
		t.Require().NotEmpty(nextPageToken)
		t.Require().ElementsMatch(records, AllSortedRecords[:pageSize])

		records = []*Example{}
		nextPageToken, err = PaginatedQuery(
			ctx, &records, t.db,
			func(d *gorm.DB) *gorm.DB { return d.Model(&Example{}) },
			pageSize, nextPageToken,
			orderByColumns,
		)
		t.Require().NoError(err)
		t.Require().NotEmpty(nextPageToken)
		t.Require().ElementsMatch(records, AllSortedRecords[pageSize:2*pageSize])
	})
}
