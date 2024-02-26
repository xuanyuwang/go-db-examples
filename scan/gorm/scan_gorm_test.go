package scangorm

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type TestScan struct {
	suite.Suite
	postres  *embeddedpostgres.EmbeddedPostgres
	db       *gorm.DB
	cacheDir string
}

func TestGORMScanSuite(t *testing.T) {
	suite.Run(t, &TestScan{})
}

// Initialize an empty database
func (t *TestScan) SetupSuite() {
	cachePath := fmt.Sprintf("embedded-postgres-go-%s", uuid.NewString())
	cacheDir, err := os.MkdirTemp("", cachePath)
	t.Require().NoError(err)
	t.cacheDir = cacheDir
	t.postres = embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().CachePath(t.cacheDir))
	err = t.postres.Start()
	t.Require().NoError(err)
	dsn := "host=localhost user=postgres password=postgres dbname=postgres sslmode=disable"
	t.db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	t.Require().NoError(err)
}

// Stop the database
func (t *TestScan) TearDownSuite() {
	err := t.postres.Stop()
	t.Require().NoError(err)
	os.RemoveAll(t.cacheDir)
}

type TimeArray struct{}

func (t *TimeArray) Scan(src interface{}) error {
	switch srcType := src.(type) {
	case []string:
		fmt.Printf("type of src is []string %v\n", srcType)
	default:
		fmt.Printf("type of src is %v\n", reflect.TypeOf(srcType))
	}
	return nil
}

// Test cases
func (t *TestScan) TestScanPrimitiveTypes() {
	type Example struct {
		gorm.Model
		IntCol    int
		FloatCol  float64
		StringCol string
		BoolCol   bool
		TimeCol   time.Time
	}
	t.db.AutoMigrate(&Example{})
	t.T().Cleanup(func() {
		t.db.Migrator().DropTable(&Example{})
	})

	t.db.Model(&Example{}).Create(&Example{
		IntCol:    1,
		FloatCol:  1.0,
		StringCol: "1",
		BoolCol:   true,
		TimeCol:   time.Unix(1000, 0),
	})
	t.Run("Read Int type", func() {
		var (
			intV       int
			int8V      int8
			int16V     int16
			int32V     int32
			int64V     int64
			uintV      uint
			uint8V     uint8
			uint16V    uint16
			uint32V    uint32
			uint64V    uint64
			stringV    string
			byteArrayV []byte
		)
		rows, err := t.db.Model(&Example{}).Limit(1).Select("int_col, int_col, int_col, int_col, int_col, int_col, int_col, int_col, int_col, int_col, int_col, int_col").Rows()
		t.Require().NoError(err)
		for rows.Next() {
			err = rows.Scan(&intV, &int8V, &int16V, &int32V, &int64V, &uintV, &uint8V, &uint16V, &uint32V, &uint64V, &stringV, &byteArrayV)
			t.Require().NoError(err)
			t.Assert().Equal(1, intV, "Wrong IntCol value")
			t.Assert().Equal(int8(1), int8V, "Wrong IntCol value")
			t.Assert().Equal(int16(1), int16V, "Wrong IntCol value")
			t.Assert().Equal(int32(1), int32V, "Wrong IntCol value")
			t.Assert().Equal(int64(1), int64V, "Wrong IntCol value")
			t.Assert().Equal(uint(1), uintV, "Wrong IntCol value")
			t.Assert().Equal(uint8(1), uint8V, "Wrong IntCol value")
			t.Assert().Equal(uint16(1), uint16V, "Wrong IntCol value")
			t.Assert().Equal(uint32(1), uint32V, "Wrong IntCol value")
			t.Assert().Equal(uint64(1), uint64V, "Wrong IntCol value")
			t.Assert().Equal("1", stringV, "Wrong IntCol value")
			t.Assert().Equal([]byte("1"), byteArrayV, "Wrong IntCol value")
		}
	})
	t.Run("Read Float type", func() {
		var (
			float32V   float32
			float64V   float64
			stringV    string
			byteArrayV []byte
		)
		rows, err := t.db.Model(&Example{}).Limit(1).Select("float_col, float_col,float_col, float_col").Rows()
		t.Require().NoError(err)
		for rows.Next() {
			err = rows.Scan(&float32V, &float64V, &stringV, &byteArrayV)
			t.Require().NoError(err)
			t.Assert().Equal(float32(1), float32V, "Wrong floatCol value")
			t.Assert().Equal(float64(1), float64V, "Wrong floatCol value")
			t.Assert().Equal("1", stringV, "Wrong floatCol value")
			t.Assert().Equal([]byte("1"), byteArrayV, "Wrong floatCol value")
		}
	})
	t.Run("Read String type", func() {
		var (
			stringV    string
			byteArrayV []byte
			intV       int
		)
		rows, err := t.db.Model(&Example{}).Limit(1).Select("string_col, string_col, string_col").Rows()
		t.Require().NoError(err)
		for rows.Next() {
			err = rows.Scan(&stringV, &byteArrayV, &intV)
			t.Require().NoError(err)
			t.Assert().Equal("1", stringV, "Wrong StringCol value")
			t.Assert().Equal([]byte("1"), byteArrayV, "Wrong StringCol value")
			t.Assert().Equal(1, intV)
		}
	})
	t.Run("Read Bool type", func() {
		var (
			boolV      bool
			stringV    string
			byteArrayV []byte
		)
		rows, err := t.db.Model(&Example{}).Limit(1).Select("bool_col, bool_col, bool_col").Rows()
		t.Require().NoError(err)
		for rows.Next() {
			err = rows.Scan(&boolV, &stringV, &byteArrayV)
			t.Require().NoError(err)
			t.Assert().Equal(true, boolV, "Wrong BoolCol value")
			t.Assert().Equal("true", stringV, "Wrong BoolCol value")
			t.Assert().Equal([]byte("true"), byteArrayV, "Wrong BoolCol value")
		}
	})
	t.Run("Read timestamp type", func() {
		var (
			timeV      time.Time
			stringV    string
			byteArrayV []byte
		)
		rows, err := t.db.Model(&Example{}).Select("time_col, time_col, time_col").Rows()
		t.Require().NoError(err)
		for rows.Next() {
			err = rows.Scan(&timeV, &stringV, &byteArrayV)
			t.Require().NoError(err)
			t.Assert().Equal(time.Unix(1000, 0), timeV, "Wrong TimeCol value")
		}
	})
}

func (t *TestScan) TestStructScan() {
	type Example struct {
		gorm.Model
		IntCol    int
		FloatCol  float64
		StringCol string
		BoolCol   bool
		TimeCol   time.Time
	}
	t.db.AutoMigrate(&Example{})
	t.T().Cleanup(func() {
		t.db.Migrator().DropTable(&Example{})
	})

	t.db.Model(&Example{}).Create(&Example{
		IntCol:    1,
		FloatCol:  1.0,
		StringCol: "1",
		BoolCol:   true,
		TimeCol:   time.Unix(1000, 0),
	})
	t.Run("Unlike Scan, a struct that doesn't have values with wrong field names", func() {
		type Result struct {
			TimeValue time.Time
		}

		var r Result
		err := t.db.Model(&Example{}).Select("int_col, time_col").Limit(1).Scan(&r).Error
		t.Require().NoError(err)
		t.Assert().NotEqual(Result{TimeValue: time.Unix(1000, 0)}, r)
		t.Assert().Equal(Result{TimeValue: time.Time{}}, r)
	})
	t.Run("A struct that has correct field names will have values", func() {
		type Result struct {
			TimeCol time.Time
		}

		var r Result
		err := t.db.Model(&Example{}).Select("int_col, time_col").Limit(1).Scan(&r).Error
		t.Require().NoError(err)
		t.Assert().Equal(Result{TimeCol: time.Unix(1000, 0)}, r)
	})
}
func (t *TestScan) TestAggResult() {
	type Example struct {
		gorm.Model
		IntCol  int
		BoolCol bool
	}
	t.db.AutoMigrate(&Example{})
	t.T().Cleanup(func() {
		t.db.Migrator().DropTable(&Example{})
	})

	t.db.Model(&Example{}).Create(&Example{IntCol: 1, BoolCol: true})
	t.db.Model(&Example{}).Create(&Example{IntCol: 1, BoolCol: false})
	db := t.db.Model(&Example{}).Select("int_col, ARRAY_AGG(bool_col) AS bools").Group("int_col").Where("int_col = 1")
	t.Run("Agg value can't be simply assigned to []T", func() {
		rows, err := db.Rows()
		t.Require().NoError(err)
		for rows.Next() {
			var firstCol int
			var secondCol []bool
			err := rows.Scan(&firstCol, &secondCol)
			t.Require().Error(err)
		}
	})
	t.Run("Agg value can be assigned to string", func() {
		rows, err := db.Rows()
		t.Require().NoError(err)
		for rows.Next() {
			var firstCol int
			var secondCol string
			err := rows.Scan(&firstCol, &secondCol)
			t.Require().NoError(err)
			t.Assert().Equal(1, firstCol)
			t.Assert().Equal("{t,f}", secondCol)
		}
	})
	t.Run("Agg value can be assigned to any type that implements Scan", func() {
		rows, err := db.Rows()
		t.Require().NoError(err)
		for rows.Next() {
			var firstCol int
			var secondCol BoolArray
			err := rows.Scan(&firstCol, &secondCol)
			t.Require().NoError(err)
			t.Assert().Equal(1, firstCol)
			t.Assert().Equal(BoolArray{Value: []bool{true, false}}, secondCol, "wrong bool arry")
		}
	})
}

type BoolArray struct {
	Value []bool
}

func (a *BoolArray) Scan(src any) error {
	switch srcType := src.(type) {
	case string:
		trimmedString := strings.Trim(src.(string), "{}")
		symbols := strings.Split(trimmedString, ",")
		a.Value = make([]bool, 0, len(symbols))
		for _, v := range symbols {
			var b bool
			if v == "t" {
				b = true
			} else if v == "f" {
				b = false
			} else {
				return fmt.Errorf("Can't convert %v to bool", v)
			}
			a.Value = append(a.Value, b)
		}
	default:
		return fmt.Errorf("Can't convert %v to []bool", srcType)
	}
	return nil
}
