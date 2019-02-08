package wtr

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type LicenceRow struct {
	LicenceNumber        string
	LicenceIssueDate     string
	SidLatNS             string
	SidLatDeg            string
	SidLatMin            string
	SidLatSec            string
	SidLongEW            string
	SidLongDeg           string
	SidLongMin           string
	SidLongSec           string
	NGR                  string
	Frequency            string
	FrequencyType        string
	StationType          string
	ChannelWidth         string
	ChannelWidthType     string
	HeightAboveSeaLevel  string
	AntennaErp           string
	AntennaErpType       string
	AntennaType          string
	AntennaGain          string
	AntennaAzimuth       string
	HorizontalElements   string
	VerticalElements     string
	AntennaHeight        string
	AntennaLocation      string
	EflUpperLower        string
	AntennaDirection     string
	AntennaElevation     string
	AntennaPolarisation  string
	AntennaName          string
	FeedingLoss          string
	FadeMargin           string
	EmissionCode         string
	ApCommentIntern      string
	Vector               string
	LicenseeSurname      string
	LicenseeFirstName    string
	LicenseeCompany      string
	Status               string
	Tradeable            string
	Publishable          string
	ProductCode          string
	ProductDescription   string
	ProductDescription31 string
	ProductDescription32 string
	Osgb36Eastings       int
	Osgb36Northings      int
	Wgs84Eastings        float64
	Wgs84Northings       float64
	// The last four values are not present in the original OFCOM csv.
	// They are can be added externally (ie. from outside this package).
	// Saving to csv will save them if they are present.
}

// newLicenceRow tidies each record before returning the LicenceRow
func newLicenceRow(row map[string]string) *LicenceRow {
	// --------------------------------------------- Product Code & Description
	productCode := strings.TrimSpace(row["Product Code"])
	productDescription := strings.TrimSpace(row["Product Description"])
	if len(productCode) > 6 && len(productDescription) == 0 {
		// Product Description may be appended to Product Code.
		// Split the Product Code appropriately.
		cre := regexp.MustCompile(`([0-9]{6})[\- ]+(.*)$`)
		mo := cre.FindStringSubmatch(productCode)
		if len(mo) > 2 {
			productCode = mo[1]
			productDescription = mo[2]
		}
	}
	// ----------------------------------------------- Tweak some company names
	// Shorter names fit better in, say, KML balloons.
	licenseeCompany := row["Licencee Company"]
	if licenseeCompany == "BRITISH TELECOMMUNICATIONS PUBLIC LIMITED COMPANY" {
		licenseeCompany = "BT PLC"
	} else if licenseeCompany == "MOBILE BROADBAND NETWORK LIMITED" {
		licenseeCompany = "MBNL"
	} else {
		pairs := []struct {
			old, new string
		}{
			{"Public Limited Company", "PLC"},
			{"PUBLIC LIMITED COMPANY", "PLC"},
			{"Limited", "Ltd"},
			{"LIMITED", "LTD"},
		}
		for _, pair := range pairs {
			if strings.Contains(licenseeCompany, pair.old) {
				licenseeCompany = strings.Replace(licenseeCompany, pair.old, pair.new, 1)
			}
		}
	}
	// ------------------------------------------------------ Normalize the NGR
	ngr := strings.Join(strings.Fields(row["NGR"]), "")

	// --------------------------------
	// These columns are present in every row.
	licenceRow := LicenceRow{
		LicenceNumber:        row["Licence Number"],
		LicenceIssueDate:     row["Licence issue date"],
		SidLatNS:             row["SID_LAT_N_S"],
		SidLatDeg:            row["SID_LAT_DEG"],
		SidLatMin:            row["SID_LAT_MIN"],
		SidLatSec:            row["SID_LAT_SEC"],
		SidLongEW:            row["SID_LONG_E_W"],
		SidLongDeg:           row["SID_LONG_DEG"],
		SidLongMin:           row["SID_LONG_MIN"],
		SidLongSec:           row["SID_LONG_SEC"],
		NGR:                  ngr,
		Frequency:            row["Frequency"],
		FrequencyType:        row["Frequency Type"],
		StationType:          row["Station Type"],
		ChannelWidth:         row["Channel Width"],
		ChannelWidthType:     row["Channel Width type"],
		HeightAboveSeaLevel:  row["Height above sea level"],
		AntennaErp:           row["Antenna ERP"],
		AntennaErpType:       row["Antenna ERP type"],
		AntennaType:          row["Antenna Type"],
		AntennaGain:          row["Antenna Gain"],
		AntennaAzimuth:       row["Antenna AZIMUTH"],
		HorizontalElements:   row["Horizontal Elements"],
		VerticalElements:     row["Vertical Elements"],
		AntennaHeight:        row["Antenna Height"],
		AntennaLocation:      row["Antenna Location"],
		EflUpperLower:        row["EFL_UPPER_LOWER"],
		AntennaDirection:     row["Antenna Direction"],
		AntennaElevation:     row["Antenna Elevation"],
		AntennaPolarisation:  row["Antenna Polarisation"],
		AntennaName:          row["Antenna Name"],
		FeedingLoss:          row["Feeding Loss"],
		FadeMargin:           row["Fade Margin"],
		EmissionCode:         row["Emission Code"],
		ApCommentIntern:      row["AP_COMMENT_INTERN"],
		Vector:               row["Vector"],
		LicenseeSurname:      row["Licencee Surname"],
		LicenseeFirstName:    row["Licencee First Name"],
		LicenseeCompany:      licenseeCompany,
		Status:               row["Status"],
		Tradeable:            row["Tradeable"],
		Publishable:          row["Publishable"],
		ProductCode:          productCode,
		ProductDescription:   productDescription,
		ProductDescription31: row["Product Description 31"],
		ProductDescription32: row["Product Description 32"],
	}

	// The following columns are not present in the original OFCOM csv but
	// may be present a munged version.
	var err error

	col := "OSGB36 E"
	if _, ok := row[col]; ok {
		licenceRow.Osgb36Eastings, err = strconv.Atoi(row["OSGB36 E"])
		if err != nil {
			log.Fatal(err)
		}
	}

	col = "OSGB36 N"
	if _, ok := row[col]; ok {
		licenceRow.Osgb36Northings, err = strconv.Atoi(row[col])
		if err != nil {
			log.Fatal(err)
		}
	}

	col = "WGS84 E"
	if _, ok := row[col]; ok {
		licenceRow.Wgs84Eastings, err = strconv.ParseFloat(row["WGS84 E"], 64)
		if err != nil {
			log.Fatal(err)
		}
	}

	col = "WGS84 N"
	if _, ok := row[col]; ok {

		licenceRow.Wgs84Northings, err = strconv.ParseFloat(row["WGS84 N"], 64)
		if err != nil {
			log.Fatal(err)
		}
	}

	return &licenceRow
}

// toMap puts all of the LicenceRow member variables in a map. These
// will only be included in the csv if the associated header column is present.
func (licenceRow *LicenceRow) toMap() map[string]string {
	return map[string]string{
		"Licence Number":         licenceRow.LicenceNumber,
		"Licence issue date":     licenceRow.LicenceIssueDate,
		"SID_LAT_N_S":            licenceRow.SidLatNS,
		"SID_LAT_DEG":            licenceRow.SidLatDeg,
		"SID_LAT_MIN":            licenceRow.SidLatMin,
		"SID_LAT_SEC":            licenceRow.SidLatSec,
		"SID_LONG_E_W":           licenceRow.SidLongEW,
		"SID_LONG_DEG":           licenceRow.SidLongDeg,
		"SID_LONG_MIN":           licenceRow.SidLongMin,
		"SID_LONG_SEC":           licenceRow.SidLongSec,
		"NGR":                    licenceRow.NGR,
		"Frequency":              licenceRow.Frequency,
		"Frequency Type":         licenceRow.FrequencyType,
		"Station Type":           licenceRow.StationType,
		"Channel Width":          licenceRow.ChannelWidth,
		"Channel Width type":     licenceRow.ChannelWidthType,
		"Height above sea level": licenceRow.HeightAboveSeaLevel,
		"Antenna ERP":            licenceRow.AntennaErp,
		"Antenna ERP type":       licenceRow.AntennaErpType,
		"Antenna Type":           licenceRow.AntennaType,
		"Antenna Gain":           licenceRow.AntennaGain,
		"Antenna AZIMUTH":        licenceRow.AntennaAzimuth,
		"Horizontal Elements":    licenceRow.HorizontalElements,
		"Vertical Elements":      licenceRow.VerticalElements,
		"Antenna Height":         licenceRow.AntennaHeight,
		"Antenna Location":       licenceRow.AntennaLocation,
		"EFL_UPPER_LOWER":        licenceRow.EflUpperLower,
		"Antenna Direction":      licenceRow.AntennaDirection,
		"Antenna Elevation":      licenceRow.AntennaElevation,
		"Antenna Polarisation":   licenceRow.AntennaPolarisation,
		"Antenna Name":           licenceRow.AntennaName,
		"Feeding Loss":           licenceRow.FeedingLoss,
		"Fade Margin":            licenceRow.FadeMargin,
		"Emission Code":          licenceRow.EmissionCode,
		"AP_COMMENT_INTERN":      licenceRow.ApCommentIntern,
		"Vector":                 licenceRow.Vector,
		"Licencee Surname":       licenceRow.LicenseeSurname,
		"Licencee First Name":    licenceRow.LicenseeFirstName,
		"Licencee Company":       licenceRow.LicenseeCompany,
		"Status":                 licenceRow.Status,
		"Tradeable":              licenceRow.Tradeable,
		"Publishable":            licenceRow.Publishable,
		"Product Code":           licenceRow.ProductCode,
		"Product Description":    licenceRow.ProductDescription,
		"Product Description 31": licenceRow.ProductDescription31,
		"Product Description 32": licenceRow.ProductDescription32,
		"OSGB36 E":               strconv.Itoa(licenceRow.Osgb36Eastings),
		"OSGB36 N":               strconv.Itoa(licenceRow.Osgb36Northings),
		"WGS84 E":                fmt.Sprintf("%f", licenceRow.Wgs84Eastings),
		"WGS84 N":                fmt.Sprintf("%f", licenceRow.Wgs84Northings),
	}
}

type LicenceRows []*LicenceRow

type LicenceCollection struct {
	Header []string
	Rows   LicenceRows
}

func LoadData(csvFileName string) *LicenceCollection {
	csvFile, err := os.Open(csvFileName)
	if err != nil {
		log.Fatalln("CSV open:", err)
	}
	defer csvFile.Close()

	return ReadCsv(csvFile)
}

// ReadCsv to read in the OFCOM WTR csv.
func ReadCsv(reader io.Reader) *LicenceCollection {
	header, rawRows := CSVToMap(bufio.NewReader(reader))

	lc := LicenceCollection{header, make(LicenceRows, len(rawRows))}
	for i, row := range rawRows {
		lc.Rows[i] = newLicenceRow(row)
	}
	return &lc
}

// WriteCsv writes the csv header, then writes the rows.
func (lc *LicenceCollection) WriteCsv(writer io.Writer) {
	w := csv.NewWriter(writer)
	if err := w.Write(lc.Header); err != nil {
		log.Fatalf("LicenceCollection.WriteCsv header: %v", err)
	}

	var csvRow = make([]string, len(lc.Header))
	for _, row := range lc.Rows {
		rowAsMap := row.toMap()
		for j, heading := range lc.Header {
			// rowAsMap[heading] checked for existence during development.
			csvRow[j] = rowAsMap[heading]
		}
		if err := w.Write(csvRow); err != nil {
			log.Fatalf("LicenceCollection.WriteCsv row: %v", err)
		}
	}
	w.Flush()
}

// GetCompanies returns a slice of strings of unique Company names from all
// the licence rows in the licence collection.
func (lc *LicenceCollection) GetCompanies() []string {
	set := make(map[string]bool)
	for _, licenceRow := range lc.Rows {
		set[licenceRow.LicenseeCompany] = true
	}

	companies := make([]string, len(set))
	i := 0
	for k := range set {
		companies[i] = k
		i++
	}
	sort.Strings(companies)

	return companies
}

type FilterFn func(licenceRow *LicenceRow) bool

// Filter returns a filtered LicenceCollection. Every filterFunc is called on
// each LicenceRow in LicenceCollection. Every filterFunc has to return true
// for the LicenceRow to be added to the filtered LicenceCollection.
func (lc *LicenceCollection) Filter(filterFuncs ...FilterFn) *LicenceCollection {
	header := lc.Header
	filtered := LicenceCollection{header, make(LicenceRows, 0)}

Loop:
	for _, row := range lc.Rows {
		for _, filterFunc := range filterFuncs {
			if !filterFunc(row) {
				break Loop
			}
		}
		// All filters returned true.
		filtered.Rows = append(filtered.Rows, row)
	}

	return &filtered
}

// FilterInPlace is as Filter but overwrites the original backing array with the
// filtered.
func (lc *LicenceCollection) FilterInPlace(filterFuncs ...FilterFn) *LicenceCollection {
	filteredRows := lc.Rows[:0]

Loop:
	for _, row := range lc.Rows {
		for _, filterFunc := range filterFuncs {
			if !filterFunc(row) {
				break Loop // not this one
			}
		}
		// All filters returned true.
		filteredRows = append(filteredRows, row)
	}
	lc.Rows = filteredRows
	return lc
}

// FilterPointToPoint is a specialised version of FilterProductCodes that
// omits the intermediate FilterFn function.
func FilterPointToPoint(row *LicenceRow) bool {
	return row.ProductCode == "301010"
}

// FilterProductCodes returns a function with the FilterFn signature. This
// function returns true a LicenceRow ProductCode matches any product code in
// productCodes.
func FilterProductCodes(productCodes ...string) func(*LicenceRow) bool {
	lookup := make(map[string]bool)
	for _, code := range productCodes {
		lookup[code] = true
	}
	return func(licenceRow *LicenceRow) bool {
		_, found := lookup[licenceRow.ProductCode]
		return found
	}
}

func FilterCompanies(companies ...string) func(*LicenceRow) bool {
	lookup := make(map[string]bool)
	for _, company := range companies {
		lookup[company] = true
	}
	return func(licenceRow *LicenceRow) bool {
		_, found := lookup[licenceRow.LicenseeCompany]
		return found
	}
}

// CSVToMap takes a reader and returns a slice of maps.
// Uses the header row as the keys.
// From a Gist on GitHub
func CSVToMap(reader io.Reader) ([]string, []map[string]string) {
	r := csv.NewReader(reader)
	var rows []map[string]string
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		if header == nil {
			header = record
		} else {
			dict := make(map[string]string, len(header))
			for i := range header {
				dict[header[i]] = record[i]
			}
			rows = append(rows, dict)
		}
	}
	return header, rows
}

func GetProductCodes() map[string]string {
	return map[string]string{
		//"250011": "Broadband Fixed Wireless Access (28 GHz- Guernsey)",
		"301010": "Fixed Links",
		"302010": "GHz CCTV",
		"304010": "Scanning Telemetry",
		"304020": "Scanning Telemetry",
		"305010": "Self Co-Ord Links",
		"306040": "Satellite (Permanent Earth Station)",
		"307030": "Satellite TES Cat1",
		"307040": "Satellite TES Cat2",
		"307050": "Satellite TES Cat3",
		"308010": "Satellite (Earth Station Network)",
		//"308030": "Satellite (Earth Station Network)",
		"308040": "Satellite (Non Fixed Satellite Earth Station)",
		"308130": "Network 2GHz Licence",
		"309010": "GNSS Repeater",
		"351010": "CSR International (Coastal Station Radio)",
		"351020": "CSR UK",
		"351030": "CSR Marina",
		"351090": "Maritime Suppliers",
		"352010": "Maritime Navaids and Radar",
		"352020": "Differential Global Positioning System",
		"352030": "AIS (Automatic Identification System)",
		"354010": "Coastal Station Radio (UK) Area Defined",
		"354020": "Coastal Station Radio (Int) Area Defined",
		"408010": "Business Radio Technically Assigned",
		"409020": "Business Radio (Public Safety Radio)",
		"409030": "Business Radio (GSM-R Railway Use)",
		"409510": "BR Area Assigned",
		"470807": "Aeronautical Station (Aeronautical Broadcast)",
		"470808": "Aeronautical Station (Aerodrome Surface and Operational",
		"502040": "Public Wireless Networks (2G Cellular Operator)",
		"502050": "Public Wireless Networks",
		"502081": "Public Wireless Networks (2G Cellular Operator - Guernsey)",
		"502082": "Public Wireless Networks (2G Cellular Operator - Isle of Man )",
		"502083": "Public Wireless Networks (2G Cellular Operator - Jersey)",
		"503010": "Spectrum Access 3.6 GHz",
		"503012": "Fixed Wireless Access (3.5 GHz - Isle of Man)",
		"503013": "Fixed Wireless Access (3.5 GHz - Jersey)",
		"503014": "Fixed Wireless Access (3.6 GHz - Guernsey)",
		"503015": "Fixed Wireless Access (3.6 GHz - Isle of Man)",
		"503016": "Fixed Wireless Access (3.6 GHz - Jersey)",
		"503017": "Fixed Wireless Access (10 GHz - Guernsey)",
		"503110": "Offshore",
		"511010": "Public Wireless Networks (3G Cellular Operator)",
		"511011": "Public Wireless Networks (3G Cellular Operator - Guernsey)",
		"511012": "Public Wireless Networks (3G Cellular Operator - Isle of Man)",
		"511013": "Public Wireless Networks (3G Cellular Operator - Jersey)",
		"513010": "Spectrum Access (3.5 GHz)",
		"521010": "Concurrent Spectrum Access (1781.7-1785 and 1876.7-1880 MHz)",
		"521020": "Spectrum Access Licence 412-414 and 422-424 MHz Bands",
		"521030": "Spectrum Access 10 - 40 GHz Bands",
		"521040": "Spectrum Access L Band (1452-1492 MHz)",
		"521050": "Spectrum Access: 28 GHz",
		"522080": "1785 MHz NI Award",
		"523010": "Spectrum Access 758 to 766 MHz",
		"523011": "Spectrum Access 542-550 MHz (Cardiff)",
		"523020": "Spectrum Access 3.4 GHz",
		"523022": "Spectrum Access 2.3 GHz",
		"525010": "Crown Recognised Spectrum Access",
		"525020": "Converted Spectrum Access",
		"541010": "Spectrum Access 800MHz and 2.6GHz",
		"551020": "Grant of RSA for Receive Only Earth Station (ROES)",
		"603020": "Miscellaneous",
		"604010": "High Duty Cycle Network Relay Points",
		"605010": "Manually Configurable White Space Devices:",
	}
}
