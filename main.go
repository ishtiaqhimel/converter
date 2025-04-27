package main

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"flag"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"
)

const (
	NullValue = "NULL"
)

type ESMeta struct {
	Index *string  `json:"_index"`
	Type  *string  `json:"_type"`
	ID    *string  `json:"_id"`
	Score *float64 `json:"_score,omitempty"`
}

type ESDoc struct {
	ESMeta
	Source map[string]interface{} `json:"_source"`
}

type FieldMapping struct {
	Index          *string                           `json:"index"`
	FieldMapping   map[string]string                 `json:"field_mapping"`
	DefaultValues  map[string]interface{}            `json:"default_values"`
	RandomGenerate map[string]map[string]interface{} `json:"random_generate"`
	File           map[string]string                 `json:"file"`
}

func main() {
	inputFile := flag.String("input", "./data/input.json", "Path to input JSON file")
	mappingFile := flag.String("mapping", "./data/mapping.json", "Path to mapping JSON file")
	outputFile := flag.String("output", "./data/output.json", "Path to output JSON file")
	limit := flag.Int("limit", -1, "Limit of documents to process (-1 for all)")
	flag.Parse()

	start := time.Now()
	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatal("failed to open file", err)
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Fatal("failed to close file", err)
		}
	}(file)

	scanner := bufio.NewScanner(file)
	var inputData []string
	count := 0
	for scanner.Scan() {
		count++
		if *limit > 0 && count > *limit {
			break
		}
		inputData = append(inputData, scanner.Text())
	}

	var docs []ESDoc
	for _, data := range inputData {
		if strings.TrimSpace(data) == "" {
			continue
		}
		var doc ESDoc
		err = json.Unmarshal([]byte(data), &doc)
		if err != nil {
			log.Fatal("failed to unmarshal input data", err)
		}
		docs = append(docs, doc)
	}

	mappingBytes, err := os.ReadFile(*mappingFile)
	if err != nil {
		log.Fatal("failed to read mapping file", err)
	}

	var mapping FieldMapping
	if err = json.Unmarshal(mappingBytes, &mapping); err != nil {
		log.Fatal("failed to unmarshal mapping file", err)
	}

	var result []string
	rn := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, doc := range docs {
		newSource := map[string]interface{}{}
		for newField, oldField := range mapping.FieldMapping {
			value := extractFieldValue(doc.Source, strings.Split(oldField, "."))
			if value != nil {
				insertFieldValue(newSource, strings.Split(newField, "."), value)
			}
		}

		for key, val := range mapping.DefaultValues {
			insertFieldValue(newSource, strings.Split(key, "."), val)
		}

		for key, config := range mapping.RandomGenerate {
			insertFieldValue(newSource, strings.Split(key, "."), generateRandomValue(rn, config))
		}

		for key, val := range mapping.File {
			fileData := map[string]interface{}{}
			dataMapByID := map[string]map[string]interface{}{}
			if key == "path" {
				if val == "" {
					log.Fatal("file path is empty for", key)
				}
				dataMapByID = extractFileData(fileData, val)
				if value, ok := dataMapByID[*doc.ID]; ok {
					for v, k := range value {
						insertFieldValue(newSource, strings.Split(v, "."), k)
					}
				}
			}
		}

		newDoc := ESDoc{
			ESMeta: ESMeta{
				Index: mapping.Index,
				Type:  doc.Type,
				ID:    doc.ID,
				Score: doc.Score,
			},
			Source: newSource,
		}
		docJson, err := json.Marshal(newDoc)
		if err != nil {
			log.Fatal("failed to marshal new doc", err)
		}
		result = append(result, string(docJson))
	}

	err = os.WriteFile(*outputFile, []byte(strings.Join(result, "\n")), 0644)
	if err != nil {
		log.Fatal("failed to write output file", err)
	}

	elapsed := time.Since(start)
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)
	log.Printf("Time taken: %s\n", elapsed)
	log.Printf("Memory used: %d MB\n", (memEnd.Alloc-memStart.Alloc)/(1024*1024))
}

func extractFieldValue(data map[string]interface{}, path []string) interface{} {
	if len(path) == 0 {
		return data
	}
	val, ok := data[path[0]]
	if !ok {
		return nil
	}
	if len(path) == 1 {
		if val == nil {
			return NullValue
		}
		return val
	}
	switch typed := val.(type) {
	case map[string]interface{}:
		return extractFieldValue(typed, path[1:])
	default:
		return nil
	}
}

func insertFieldValue(data map[string]interface{}, path []string, value interface{}) {
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		if _, exists := data[key]; !exists {
			data[key] = make(map[string]interface{})
		}
		data = data[key].(map[string]interface{})
	}
	if value == NullValue {
		value = nil
	}
	data[path[len(path)-1]] = value
}

func extractFileData(fileData map[string]interface{}, filePath string) map[string]map[string]interface{} {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Fatal("failed to close file", err)
		}
	}(file)

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	headers := records[0]
	idIndex := -1
	for i, header := range headers {
		if header == "id" {
			idIndex = i
			break
		}
	}

	if idIndex == -1 {
		log.Fatal("id column not found")
	}

	dataMapByID := make(map[string]map[string]interface{})
	for _, row := range records[1:] {
		id := row[idIndex]
		fields := make(map[string]interface{})
		for i, header := range headers {
			if i != idIndex {
				fields[header] = row[i]
			}
		}
		dataMapByID[id] = fields
	}
	return dataMapByID
}

func generateRandomValue(rn *rand.Rand, config map[string]interface{}) interface{} {
	switch config["type"] {
	case "binary":
		data := make([]byte, 64)
		rn.Read(data)
		return base64.StdEncoding.EncodeToString(data)

	case "boolean":
		return rn.Intn(2) == 0

	case "date":
		// TODO: need to implement

	case "long", "integer", "short", "byte":
		mn := int(config["min"].(float64))
		mx := int(config["max"].(float64))
		return rn.Intn(mx-mn+1) + mn

	case "double", "float", "half_float":
		mn := config["min"].(float64)
		mx := config["max"].(float64)
		d := mn + rn.Float64()*(mx-mn)
		return math.Round(d*100) / 100

	case "keyword", "wildcard", "constant_keyword":
		values := config["values"].([]interface{})
		return values[rn.Intn(len(values))] // TODO: generate complete random value

	default:
		return nil
	}
	return struct{}{}
}
