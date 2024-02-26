package scanpq

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"database/sql"
	"database/sql/driver"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"

	_ "github.com/lib/pq"
)

type TestScan struct {
	suite.Suite
	postres  *embeddedpostgres.EmbeddedPostgres
	db       *sql.DB
	cacheDir string
}

func TestPqScanSuite(t *testing.T) {
	suite.Run(t, &TestScan{})
}

func (t *TestScan) SetupSuite() {
	cachePath := fmt.Sprintf("embedded-postgres-go-%s", uuid.NewString())
	cacheDir, err := os.MkdirTemp("", cachePath)
	t.Require().NoError(err)
	t.cacheDir = cacheDir
	t.postres = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().CachePath(t.cacheDir))
	err = t.postres.Start()
	t.Require().NoError(err)
	dsn := "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable"
	t.db, err = sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
}

func (t *TestScan) TearDownSuite() {
	err := t.postres.Stop()
	t.Require().NoError(err)
}

type TimeArray struct {
	Values []uint8
}

func (t *TimeArray) Scan(src interface{}) error {
	switch srcType := src.(type) {
	case []uint8:
		t.Values = make([]uint8, len(src.([]uint8)))
		copy(t.Values, src.([]uint8))
	default:
		fmt.Printf("can't convert type of src: %v\n", reflect.TypeOf(srcType))
	}
	return nil
}

func (t *TimeArray) Value() (driver.Value, error) {
	return t.Values, nil
}

// The agg result returned by driver `pq` is of type []uint8
func (t *TestScan) TestAggResult() {
	query := `
	create table example (int_col int, time_col timestamp);
	insert into example values (1, '2000-01-01'), (1, '3000-01-01');
	`
	t.db.Exec(query)

	var firstV int
	var timeA TimeArray

	rows, err := t.db.Query("select int_col, ARRAY_AGG(time_col) from example group by int_col")
	t.Require().NoError(err)
	for rows.Next() {
		rows.Scan(&firstV, &timeA)
		t.Assert().Equal(1, firstV)
		t.Assert().Equal([]uint8(`{"2000-01-01 00:00:00","3000-01-01 00:00:00"}`), timeA.Values)
	}
}
