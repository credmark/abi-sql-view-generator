package utils

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"text/template"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const (
	fnInitialLength       = 11
	indexedInputLength    = 70
	unindexedInputLength  = 3
	individualInputLength = 64
)

type AbiContract struct {
	// Events is a slice of AbiEvent struct
	Events  []AbiEvent
	// Methods is a slice of AbiMethod struct
	Methods []AbiMethod
}

type AbiContractColumn struct {
	// ColumnName is the name of the column from which to extract and decode the input 
	// field from. i.e. ('topics' from the logs table if address is indexed)
	ColumnName string
	// InputName refers to the name of the input (argument) to the event or method
	InputName  string
	// InputType is the data type of the input to the event or method
	InputType  string
	// StartPos is the starting position from which to extract information from the hex data
	StartPos   int
	// Length is the length of characters to extract from the starting position
	Length     int
}

type AbiEvent struct {
	// ContractAddress is the contract address that the event belongs to
	ContractAddress string
	// Name is the name of the event
	Name            string
	// Inputs is the slice of AbiContractColumn which are the inputs of the event
	Inputs          []AbiContractColumn
	// SigHash is the hash of the event signature
	SigHash         string
	// Namespace is the namespace prefix added to the name of the SQL view
	Namespace       string
}

type AbiMethod struct {
	// ContractAddress is the contract address that the method belongs to
	ContractAddress string
	// Name is the name of the method
	Name            string
	// Inputs is the slice of AbiContractColumn which contains the inputs of the method
	Inputs          []AbiContractColumn
	// MethodIdHash is the hash of the method ID
	MethodIdHash    string
	// Namespace is the namespace prefix added to the name of the SQL view
	Namespace       string
}

func NewAbiContract(contractAddress string, abi abi.ABI, namespace string) *AbiContract {
	return &AbiContract{
		Events:  newAbiEvents(abi, contractAddress, namespace),
		Methods: newAbiMethods(abi, contractAddress, namespace),
	}
}

func newAbiEvent(event abi.Event, contractAddress string, namespace string) *AbiEvent {
	return &AbiEvent{
		ContractAddress: contractAddress,
		Name:            event.Name,
		SigHash:         event.ID.Hex(),
		Inputs:          createInputs(event.Inputs, "event"),
		Namespace:       namespace,
	}
}

func newAbiEvents(abi abi.ABI, contractAddress string, namespace string) []AbiEvent {
	newEvents := []AbiEvent{}
	for _, event := range abi.Events {
		newEvents = append(newEvents, *newAbiEvent(event, contractAddress, namespace))
	}

	return newEvents
}

func newAbiMethod(method abi.Method, contractAddress string, namespace string) *AbiMethod {
	return &AbiMethod{
		ContractAddress: contractAddress,
		Name:            method.Name,
		MethodIdHash:    getMethodIdHash(method.ID),
		Inputs:          createInputs(method.Inputs, "function"),
		Namespace:       namespace,
	}
}

func newAbiMethods(abi abi.ABI, contractAddress string, namespace string) []AbiMethod {
	newMethods := []AbiMethod{}
	for _, method := range abi.Methods {
		newMethods = append(newMethods, *newAbiMethod(method, contractAddress, namespace))
	}

	return newMethods
}

func createInputs(inputs abi.Arguments, typ string) []AbiContractColumn {
	newInputs := make([]AbiContractColumn, len(inputs))
	for idx, input := range inputs {
		newInputs[idx] = createInput(input, typ, idx)
	}

	return newInputs
}

func createInput(input abi.Argument, typ string, idx int) AbiContractColumn {
	return AbiContractColumn{
		ColumnName: getColumnName(typ, input.Indexed),
		InputName:  input.Name,
		InputType:  input.Type.String(),
		StartPos:   calculateStartPos(idx, input.Indexed, typ),
		Length:     individualInputLength,
	}
}

func getMethodIdHash(methodId []byte) string {
	return fmt.Sprintf("0x%s", hex.EncodeToString(methodId))
}

func getColumnName(inputType string, indexed bool) string {
	switch inputType {
	case "function":
		return "input"
	case "event":
		if indexed {
			return "topics"
		} else {
			return "data"
		}
	default:
		log.Fatal("error: unknown input type")
		return ""
	}
}

func calculateStartPos(idx int, indexed bool, typ string) int {
	switch typ {
	case "function":
		return fnInitialLength + (idx * individualInputLength)
	case "event":
		if indexed {
			return indexedInputLength + (idx * (individualInputLength + 3))
		} else {
			return unindexedInputLength + (idx * individualInputLength)
		}
	default:
		log.Fatal("error: unknown input type")
		return 0
	}
}

func (c *AbiContract) GenerateSql() {
	for _, v := range c.Events {
		log.Printf("generating SQL statement for event: %s\n", v.Name)
		v.generateSql()
	}

	for _, v := range c.Methods {
		log.Printf("generating SQL statement for function: %s\n", v.Name)
		v.generateSql()
	}
}

func (e *AbiEvent) generateSql() {
	outputPath, err := filepath.Abs("out")
	if err != nil {
		log.Fatal(err)
	}

	filename := fmt.Sprintf("%s_%s_evt_%s.sql", e.Namespace, e.ContractAddress, e.Name)
	fpath, err := filepath.Abs("templates/event.sql")
	if err != nil {
		log.Fatal(err)
	}

	t, err := template.New("event.sql").ParseFiles(fpath)
	if err != nil {
		log.Fatal(err)
	}

	fp := filepath.Join(outputPath, filename)
	f, err := os.Create(fp)
	if err != nil {
		log.Fatal(err)
	}

	err = t.Execute(f, e)
	if err != nil {
		log.Fatal(err)
	}
}

func (m *AbiMethod) generateSql() {
	outputPath, err := filepath.Abs("out")
	if err != nil {
		log.Fatal(err)
	}

	filename := fmt.Sprintf("%s_%s_fn_%s.sql", m.Namespace, m.ContractAddress, m.Name)
	fpath, err := filepath.Abs("templates/function.sql")
	if err != nil {
		log.Fatal(err)
	}

	t, err := template.New("function.sql").ParseFiles(fpath)
	if err != nil {
		log.Fatal(err)
	}

	fp := filepath.Join(outputPath, filename)
	f, err := os.Create(fp)
	if err != nil {
		log.Fatal(err)
	}

	err = t.Execute(f, m)
	if err != nil {
		log.Fatal(err)
	}
}
