package wtrcsv

import (
	"bufio"
	"bytes"
	"github.com/pkg/errors"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

const fileURL string = "http://static.ofcom.org.uk/static/radiolicensing/html/register/WTR.csv"

func compareHeaders(collection1 *Collection, collection2 *Collection) bool {
	if collection1 == collection2 {
		return false // Should not compare collection to itself.
	}
	return len(collection1.Header) == len(collection2.Header)
}

func compareRowLengths(collection1 *Collection, collection2 *Collection) bool {
	if collection1 == collection2 {
		return false // Should not compare collection to itself.
	}
	return len(collection1.Rows) == len(collection2.Rows)
}

// TestWTR does all the testing as the initial load of the data is expensive.
func TestWTR(t *testing.T) {
	// test_data contains real data. It may be out of date.
	_, filePath, _, _ := runtime.Caller(0)

	dataRoot := path.Join(path.Dir(filePath), "test_data")

	// Create test_data directory if not present.
	if _, err := os.Stat(dataRoot); os.IsNotExist(err) {
		err = os.Mkdir(dataRoot, 0755)
		if err != nil {
			t.Fatalf("%v", errors.Wrap(err, "failed to create temporary test directory"))
		}
	}
	dataPath := path.Join(dataRoot, "WTR.csv")

	// ------------------------------------------- download data if not present
	// Not really a test.
	t.Run("Download data",
		func(t *testing.T) {

			if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
				t.Skip("Test data exists. No download performed. Test skipped")
			}

			resp, err := http.Get(fileURL)
			if err != nil {
				t.Fatalf("%v", errors.Wrapf(err, "could not GET URL: \"%s\"", fileURL))
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("bad http status: %s", resp.Status)
			}

			out, err := os.Create(dataPath)
			if err != nil {
				t.Fatalf("%v", errors.Wrapf(err, "could not create file \"%s\"", dataPath))
			}
			defer out.Close()

			_, err = io.Copy(out, resp.Body)
			if err != nil {
				t.Fatalf("%v", errors.Wrap(err, "failed to copy body"))
			}
		})

	// --------------------------------------------------------- load the data
	csvFile, err := os.Open(dataPath)
	if err != nil {
		log.Fatalln(errors.Wrapf(err, "could not open csv file: \"%s\"", dataPath))
	}
	defer csvFile.Close()

	collection := ReadCSV(csvFile)
	if len(collection.Rows) == 0 {
		t.Fatal("Failed to read licence file")
	}

	// -------------------------------------- write the data back (to a buffer)
	t.Run("Write back",
		func(t *testing.T) {
			b := new(bytes.Buffer)
			writer := bufio.NewWriter(b)

			collection.WriteCSV(writer)
			if writer.Size() == 0 {
				t.Fatal("Failed to write licence file")
			}

			// Check the header row
			s1 := b.String()
			const s2 = "Licence Number,Licence issue date,SID_LAT_N_S,SID_LAT_DEG,SID_LAT_MIN,SID_LAT_SEC,SID_LONG_E_W,SID_LONG_DEG,SID_LONG_MIN,SID_LONG_SEC,NGR,Frequency,Frequency Type,Station Type,Channel Width,Channel Width type,Height above sea level,Antenna ERP,Antenna ERP type,Antenna Type,Antenna Gain,Antenna AZIMUTH,Horizontal Elements,Vertical Elements,Antenna Height,Antenna Location,EFL_UPPER_LOWER,Antenna Direction,Antenna Elevation,Antenna Polarisation,Antenna Name,Feeding Loss,Fade Margin,Emission Code,AP_COMMENT_INTERN,Vector,Licencee Surname,Licencee First Name,Licencee Company,Status,Tradeable,Publishable,Product Code,Product Description,Product Description 31,Product Description 32"
			if s1[:len(s2)] != s2 {
				t.Fatal("Header wrong")
			}

			cre := regexp.MustCompile("([^\r\n])*[\r\n]")
			header := cre.FindString(s1)
			if len(strings.Split(header, ",")) != 46 {
				t.Fatalf("Header wrong number of columns: %v", len(strings.Split(s1, ",")))
			}
		})

	// ----------------------------------------------------------------- Header
	t.Run("Header",
		func(t *testing.T) {
			if len(collection.Header) != 46 {
				t.Fatalf("Header wrong number of columns: %v", len(collection.Header))
			}
		})
	// -------------------------------------------------------- Licence Numbers
	creLicenceNumber := regexp.MustCompile("^(ES)?[0-9]{7}/[0-9]")
	t.Run("Licence numbers",
		func(t *testing.T) {
			for _, row := range collection.Rows {
				if !creLicenceNumber.MatchString(row.LicenceNumber) {
					t.Log(row.LicenceNumber)
				}
			}
		})
	// --------------------------------------------- Product Code & Description
	t.Run("Product Codes & Description",
		func(t *testing.T) {
			knownCodes := GetProductCodeLookup()
			foundCodes := make(map[string]bool)

			// Check the Product Code is known
			for _, row := range collection.Rows {
				productCode := row.ProductDescription31
				if _, ok := knownCodes[productCode]; !ok {
					t.Fatalf("unknown Product Code: \"%v\"", productCode)
				}
				foundCodes[productCode] = true
			}
			// Check that known Product Codes have been found.
			for productCode := range knownCodes {
				if _, ok := foundCodes[productCode]; !ok {
					t.Fatalf("known Product Code not used: \"%v\"", productCode)
				}
			}

			// Check that numerical product codes are the correct length
			// Check that there is a Product Description
			for _, row := range collection.Rows {
				// Numerical product code is in Product Description 31
				if len(row.ProductDescription31) != 6 {
					t.Fatalf("incorrect Product Code length: \"%v\"", row.ProductDescription31)
				}
				if len(row.ProductDescription) == 0 && len(row.ProductDescription32) == 0 {
					t.Fatal("missing Product Description")
				}
				if len(row.ProductDescription) > 0 && len(row.ProductDescription32) > 0 {
					t.Fatal("unexpected Product Description")
				}
			}
		})
	// ----------------------------------------------------- partition the data
	var collectionP2P *Collection

	t.Run("filter Product Code",
		func(t *testing.T) {
			collectionP2P = collection.Filter(FilterPointToPoint)

			if !compareHeaders(collectionP2P, collection) {
				t.Fatal("Filter did not copy headers")
			}

			// Rows should be different lengths.
			if compareRowLengths(collectionP2P, collection) {
				t.Fatal("Filter did not filter")
			}

			// Apply the same filter again.
			collection2 := collectionP2P.Filter(FilterPointToPoint)

			if !compareHeaders(collection2, collectionP2P) {
				t.Fatal("2nd Filter did not copy headers")
			}

			// Should be identical lengths.
			if !compareRowLengths(collection2, collectionP2P) {
				t.Fatal("2nd Filter filtered (it should not have done anything")
			}

			collection3 := collection.Filter(FilterNumericalProductCodes("301010"), FilterValidNGR)
			if !compareHeaders(collection3, collectionP2P) {
				t.Fatal("3rd Filter did not copy headers")
			}

			// Should be identical lengths.
			if !compareRowLengths(collection3, collectionP2P) {
				t.Fatal("3rd Filter filtered incorrectly (should have been identical to first)")
			}
		})
	// ------------------------------------------------------------------------
	t.Run("filterInPlace Product Code",
		func(t *testing.T) {
			collectionP2P = collection.Filter(FilterPointToPoint)

			if !compareHeaders(collectionP2P, collection) {
				t.Fatal("Filter did not copy headers")
			}

			// They should be different lengths.
			if compareRowLengths(collectionP2P, collection) {
				t.Fatal("Filter did not filter")
			}

			count := 0
			for _, row := range collectionP2P.Rows {
				// The numerical product code is in Product Description 31
				if row.ProductDescription31 == "301010" {
					count++
				}
			}

			if count != len(collectionP2P.Rows) {
				t.Fatal("Filter P2P count did not match")
			}

			rows := make([]*Row, len(collection.Rows))
			copy(rows, collection.Rows)
			collection2 := &Collection{collection.Header, rows}

			collection2.FilterInPlace(FilterNumericalProductCodes("301010"), FilterValidNGR)

			if count != len(collection2.Rows) {
				t.Fatal("FilterInPlace count did not match")
			}

			if compareRowLengths(collection, collection2) {
				t.Fatalf("FilterInPlace did not work (1) %v %v %v",
					len(collection.Rows),
					len(collection2.Rows),
					len(collectionP2P.Rows))
			}
			if !compareRowLengths(collectionP2P, collection2) {
				t.Fatalf("FilterInPlace did not work (2): %v, %v",
					len(collectionP2P.Rows),
					len(collection2.Rows))
			}
		})
	// ------------------------------------------------------------------------
	t.Run("filter Licensee Companies",
		func(t *testing.T) {
			companies := collection.GetCompanies()

			const company1, company2 = "MOBILE BROADBAND NETWORK LIMITED", "Vodafone Limited"
			found1, found2 := false, false
			// Ensure that the companies actually exist.
			for i := range companies {
				if companies[i] == company1 {
					found1 = true
				} else if companies[i] == company2 {
					found2 = true
				}
				if found1 && found2 {
					break
				}
			}
			if !found1 {
				t.Fatalf("Could not find company \"%v\"", company1)
			}
			if !found2 {
				t.Fatalf("Could not find company \"%v\"", company2)
			}

			collectionCustomer1 := collection.Filter(FilterCompanies(company1))
			collectionCustomer2 := collection.Filter(FilterCompanies(company2))

			if !compareHeaders(collectionCustomer1, collection) {
				t.Fatal("Filter 1 did not copy headers")
			}
			if !compareHeaders(collectionCustomer2, collection) {
				t.Fatal("Filter 2 did not copy headers")
			}

			rowCount1 := len(collectionCustomer1.Rows)
			rowCount2 := len(collectionCustomer2.Rows)
			if rowCount1 == len(collection.Rows) {
				t.Fatal("Filter 1 did not filter")
			}
			if rowCount2 == len(collection.Rows) {
				t.Fatal("Filter 2 did not filter")
			}

			collectionCustomerBoth := collection.Filter(FilterCompanies(company1, company2))

			if !compareHeaders(collectionCustomerBoth, collection) {
				t.Fatal("Filter 3 did not copy headers")
			}

			if len(collectionCustomerBoth.Rows) != (rowCount1 + rowCount2) {
				t.Fatal("Multiple filter messed up")
			}
		})
}
