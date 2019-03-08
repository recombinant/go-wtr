package wtrcsv

import (
	"bufio"
	"encoding/csv"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
)

type Row struct {
	LicenceNumber          string
	LicenceIssueDate       string
	SidLatNS               string
	SidLatDeg              string
	SidLatMin              string
	SidLatSec              string
	SidLongEW              string
	SidLongDeg             string
	SidLongMin             string
	SidLongSec             string
	NGR                    string
	Frequency              string
	FrequencyType          string
	StationType            string
	ChannelWidth           string
	ChannelWidthType       string
	HeightAboveSeaLevel    string
	AntennaErp             string
	AntennaErpType         string
	AntennaType            string
	AntennaGain            string
	AntennaAzimuth         string
	HorizontalElements     string
	VerticalElements       string
	AntennaHeight          string // Resolution to 0.5m
	AntennaLocation        string
	EflUpperLower          string
	AntennaDirection       string
	AntennaElevation       string
	AntennaPolarisation    string
	AntennaName            string
	FeedingLoss            string
	FadeMargin             string
	EmissionCode           string
	ApCommentIntern        string
	Vector                 string
	LicenseeSurname        string
	LicenseeFirstName      string
	LicenseeCompany        string
	Status                 string
	Tradeable              string
	Publishable            string
	ProductCode            string
	ProductDescription     string
	ProductDescription31   string
	ProductDescription32   string
	Wgs84LongitudeAsString string // Persistent representation
	Wgs84LatitudeAsString  string
	Wgs84Longitude         float64 // Converted from persistent
	Wgs84Latitude          float64
	OsEasting              int
	OsNorthing             int
	// The last two values are not present in the original OFCOM csv.
	// They are can be added externally (ie. from outside this package).
	// Saving to csv will save them if they are present.
}

const (
	HeadingOsEasting      = "OS Easting"
	HeadingOsNorthing     = "OS Northing"
	HeadingWgs84Longitude = "WGS84 Longitude"
	HeadingWgs84Latitude  = "WGS84 Latitude"
)

// newRow tidies each record before returning the Row
func newRow(columns map[string]string) *Row {
	// The columns in this map are present in every columns.
	row := Row{
		LicenceNumber:        columns["Licence Number"],
		LicenceIssueDate:     columns["Licence issue date"],
		SidLatNS:             columns["SID_LAT_N_S"],
		SidLatDeg:            columns["SID_LAT_DEG"],
		SidLatMin:            columns["SID_LAT_MIN"],
		SidLatSec:            columns["SID_LAT_SEC"],
		SidLongEW:            columns["SID_LONG_E_W"],
		SidLongDeg:           columns["SID_LONG_DEG"],
		SidLongMin:           columns["SID_LONG_MIN"],
		SidLongSec:           columns["SID_LONG_SEC"],
		NGR:                  columns["NGR"],
		Frequency:            columns["Frequency"],
		FrequencyType:        columns["Frequency Type"],
		StationType:          columns["Station Type"],
		ChannelWidth:         columns["Channel Width"],
		ChannelWidthType:     columns["Channel Width type"],
		HeightAboveSeaLevel:  columns["Height above sea level"],
		AntennaErp:           columns["Antenna ERP"],
		AntennaErpType:       columns["Antenna ERP type"],
		AntennaType:          columns["Antenna Type"],
		AntennaGain:          columns["Antenna Gain"],
		AntennaAzimuth:       columns["Antenna AZIMUTH"],
		HorizontalElements:   columns["Horizontal Elements"],
		VerticalElements:     columns["Vertical Elements"],
		AntennaHeight:        columns["Antenna Height"],
		AntennaLocation:      columns["Antenna Location"],
		EflUpperLower:        columns["EFL_UPPER_LOWER"],
		AntennaDirection:     columns["Antenna Direction"],
		AntennaElevation:     columns["Antenna Elevation"],
		AntennaPolarisation:  columns["Antenna Polarisation"],
		AntennaName:          columns["Antenna Name"],
		FeedingLoss:          columns["Feeding Loss"],
		FadeMargin:           columns["Fade Margin"],
		EmissionCode:         columns["Emission Code"],
		ApCommentIntern:      columns["AP_COMMENT_INTERN"],
		Vector:               columns["Vector"],
		LicenseeSurname:      columns["Licencee Surname"],
		LicenseeFirstName:    columns["Licencee First Name"],
		LicenseeCompany:      columns["Licencee Company"],
		Status:               columns["Status"],
		Tradeable:            columns["Tradeable"],
		Publishable:          columns["Publishable"],
		ProductCode:          columns["Product Code"],
		ProductDescription:   columns["Product Description"],
		ProductDescription31: columns["Product Description 31"],
		ProductDescription32: columns["Product Description 32"],
	}

	// The following columns are not present in the original OFCOM csv but
	// may be present a munged version.
	var err error

	if _, ok := columns[HeadingOsEasting]; ok {
		row.OsEasting, err = strconv.Atoi(columns[HeadingOsEasting])
		if err != nil {
			log.Fatalf("%v", errors.Wrap(err, "could not convert easting"))
		}
	}

	if _, ok := columns[HeadingOsNorthing]; ok {
		row.OsNorthing, err = strconv.Atoi(columns[HeadingOsNorthing])
		if err != nil {
			log.Fatalf("%v", errors.Wrap(err, "could not convert northing"))
		}
	}

	if _, ok := columns[HeadingWgs84Longitude]; ok {
		row.Wgs84LongitudeAsString = columns[HeadingWgs84Longitude]
		row.Wgs84Longitude, err = strconv.ParseFloat(row.Wgs84LongitudeAsString, 64)
		if err != nil {
			log.Fatalf("%v", errors.Wrap(err, "could not convert WGS84 longitude"))
		}
	}

	if _, ok := columns[HeadingWgs84Latitude]; ok {
		row.Wgs84LatitudeAsString = columns[HeadingWgs84Latitude]
		row.Wgs84Latitude, err = strconv.ParseFloat(row.Wgs84LatitudeAsString, 64)
		if err != nil {
			log.Fatalf("%v", errors.Wrap(err, "could not convert WGS84 latitude"))
		}
	}

	return &row
}

// toMap puts all of the Row member variables in a map (ie. columns). These
// will only be included in the csv if the associated header column is present.
func (row *Row) toMap() map[string]string {
	return map[string]string{
		"Licence Number":         row.LicenceNumber,
		"Licence issue date":     row.LicenceIssueDate,
		"SID_LAT_N_S":            row.SidLatNS,
		"SID_LAT_DEG":            row.SidLatDeg,
		"SID_LAT_MIN":            row.SidLatMin,
		"SID_LAT_SEC":            row.SidLatSec,
		"SID_LONG_E_W":           row.SidLongEW,
		"SID_LONG_DEG":           row.SidLongDeg,
		"SID_LONG_MIN":           row.SidLongMin,
		"SID_LONG_SEC":           row.SidLongSec,
		"NGR":                    row.NGR,
		"Frequency":              row.Frequency,
		"Frequency Type":         row.FrequencyType,
		"Station Type":           row.StationType,
		"Channel Width":          row.ChannelWidth,
		"Channel Width type":     row.ChannelWidthType,
		"Height above sea level": row.HeightAboveSeaLevel,
		"Antenna ERP":            row.AntennaErp,
		"Antenna ERP type":       row.AntennaErpType,
		"Antenna Type":           row.AntennaType,
		"Antenna Gain":           row.AntennaGain,
		"Antenna AZIMUTH":        row.AntennaAzimuth,
		"Horizontal Elements":    row.HorizontalElements,
		"Vertical Elements":      row.VerticalElements,
		"Antenna Height":         row.AntennaHeight,
		"Antenna Location":       row.AntennaLocation,
		"EFL_UPPER_LOWER":        row.EflUpperLower,
		"Antenna Direction":      row.AntennaDirection,
		"Antenna Elevation":      row.AntennaElevation,
		"Antenna Polarisation":   row.AntennaPolarisation,
		"Antenna Name":           row.AntennaName,
		"Feeding Loss":           row.FeedingLoss,
		"Fade Margin":            row.FadeMargin,
		"Emission Code":          row.EmissionCode,
		"AP_COMMENT_INTERN":      row.ApCommentIntern,
		"Vector":                 row.Vector,
		"Licencee Surname":       row.LicenseeSurname,
		"Licencee First Name":    row.LicenseeFirstName,
		"Licencee Company":       row.LicenseeCompany,
		"Status":                 row.Status,
		"Tradeable":              row.Tradeable,
		"Publishable":            row.Publishable,
		"Product Code":           row.ProductCode,
		"Product Description":    row.ProductDescription,
		"Product Description 31": row.ProductDescription31, // Product code number
		"Product Description 32": row.ProductDescription32,
		HeadingOsEasting:         strconv.Itoa(row.OsEasting),
		HeadingOsNorthing:        strconv.Itoa(row.OsNorthing),
		HeadingWgs84Longitude:    row.Wgs84LongitudeAsString,
		HeadingWgs84Latitude:     row.Wgs84LatitudeAsString,
	}
}

type Collection struct {
	Header []string
	Rows   []*Row
}

func LoadData(csvFileName string) *Collection {
	csvFile, err := os.Open(csvFileName)
	if err != nil {
		log.Fatalln(errors.Wrapf(err, "could not open csv file: \"%s\"", csvFileName))
	}
	defer csvFile.Close()

	return ReadCSV(csvFile)
}

// ReadCSV to read in the OFCOM WTR csv.
func ReadCSV(reader io.Reader) *Collection {
	header, rawColumns := CSVToMap(bufio.NewReader(reader))

	collection := Collection{header, make([]*Row, len(rawColumns))}
	for i, columns := range rawColumns {
		collection.Rows[i] = newRow(columns)
	}
	return &collection
}

// WriteCSV writes the csv header, then writes the rows.
func (collection *Collection) WriteCSV(writer io.Writer) {
	w := csv.NewWriter(writer)
	if err := w.Write(collection.Header); err != nil {
		log.Fatalf("%v", errors.Wrap(err, "could not write CSV header"))
	}

	var csvRow = make([]string, len(collection.Header))
	for _, row := range collection.Rows {
		rowAsMap := row.toMap()
		for j, heading := range collection.Header {
			// rowAsMap[heading] checked for existence during development.
			csvRow[j] = rowAsMap[heading]
		}
		if err := w.Write(csvRow); err != nil {
			log.Fatalf("%v", errors.Wrap(err, "could not write CSV row"))
		}
	}
	w.Flush()
}

// GetCompanies returns a slice of strings of unique Company names from all
// the licence rows in the licence collection.
func (collection *Collection) GetCompanies() []string {
	set := make(map[string]bool)
	for _, row := range collection.Rows {
		set[row.LicenseeCompany] = true
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

type FilterFn func(row *Row) bool

// Filter returns a filtered Collection. Every filterFunc is called on
// each Row in Collection. Every filterFunc has to return true
// for the Row to be added to the filtered Collection.
func (collection *Collection) Filter(filterFuncs ...FilterFn) *Collection {
	header := collection.Header
	filtered := Collection{header, make([]*Row, 0)}

	// All filters must return true for a row to be appended.
	for _, row := range collection.Rows {
		ok := true
		for _, filterFunc := range filterFuncs {
			if !filterFunc(row) {
				ok = false
				break // not this row
			}
		}

		if ok {
			filtered.Rows = append(filtered.Rows, row)
		}
	}

	return &filtered
}

// FilterInPlace is as Filter but overwrites the original backing array with the
// filtered.
func (collection *Collection) FilterInPlace(filterFuncs ...FilterFn) *Collection {
	filteredRows := collection.Rows[:0]

	// All filters must return true for a row to be appended.
	for _, row := range collection.Rows {
		ok := true
		for _, filterFunc := range filterFuncs {
			if !filterFunc(row) {
				ok = false
				break // not this row
			}
		}

		if ok {
			// All filters returned true.
			filteredRows = append(filteredRows, row)
		}
	}
	collection.Rows = filteredRows
	return collection
}

var creNGR = regexp.MustCompile("[A-Z]{2} ?[0-9]{5} ?[0-9]{5}$")

// FilterPointToPoint is a specialised version of FilterNumericalProductCodes that
// omits the intermediate FilterFn function.
func FilterPointToPoint(row *Row) bool {
	return row.ProductDescription31 == "301010" && creNGR.MatchString(row.NGR)
}

// FilterValidNGR ensures that there is a valid NGR
func FilterValidNGR(row *Row) bool {
	return creNGR.MatchString(row.NGR)
}

// FilterNumericalProductCodes returns a function with the FilterFn signature.
// The returned function returns true if a Row numerical product code
// matches any numerical product code in numericalProductCodes.
func FilterNumericalProductCodes(numericalProductCodes ...string) func(*Row) bool {
	lookup := make(map[string]bool)
	for _, code := range numericalProductCodes {
		lookup[code] = true
	}
	return func(row *Row) bool {
		// Numerical product code is in Product Description 31
		_, found := lookup[row.ProductDescription31]
		return found
	}
}

func FilterCompanies(companies ...string) func(*Row) bool {
	lookup := make(map[string]bool)
	for _, company := range companies {
		lookup[company] = true
	}
	return func(row *Row) bool {
		_, found := lookup[row.LicenseeCompany]
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
			log.Fatalf("%v", errors.Wrap(err, "could not read from reader"))
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

// GetProductCodeLookup returns a map of numerical product code vs
// product description (not OFCOM's verbatim).
func GetProductCodeLookup() map[string]string {
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
		"351010": "Coastal Station Radio International",
		"351020": "Coastal Station Radio UK",
		"351030": "Coastal Station Radio Marina",
		"351090": "Maritime Suppliers",
		"352010": "Maritime Navaids and Radar",
		"352020": "Differential Global Positioning System",
		"352030": "Automatic Identification System",
		"354010": "Coastal Station Radio (UK) Area Defined",
		"354020": "Coastal Station Radio (Int) Area Defined",
		"408010": "Business Radio Technically Assigned",
		"409020": "Business Radio (Public Safety Radio)",
		"409030": "Business Radio (GSM-R Railway Use)",
		"409510": "Business Radio Area Defined", // Assigned
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
		"605010": "Manually Configurable White Space Devices",
	}
}

func (row *Row) AntennaHeightAsFloat() float64 {
	height, err := strconv.ParseFloat(row.AntennaHeight, 64)
	if err != nil {
		return 0.0
	}
	return height
}

func (row *Row) FrequencyAsFloat() float64 {
	frequency, err := strconv.ParseFloat(row.Frequency, 64)
	if err != nil {
		return 0.0
	}
	return frequency
}
