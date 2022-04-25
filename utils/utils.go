package utils

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"text/template"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const (
	fnInitialLength              = 11
	indexedInputLength           = 70
	unindexedInputLength         = 3
	individualInputLength        = 64
	snowflakeIdentifierMaxLength = 255
)

type AbiContract struct {
	// Events is a slice of AbiEvent struct
	Events []AbiEvent
	// Methods is a slice of AbiMethod struct
	Methods []AbiMethod
	// Skip entire contract if there are validation issues encountered (i.e. input or event names too long)
	Skip bool
}

type AbiContractColumn struct {
	// InputName refers to the name of the input (argument) to the event or method
	Name string `json:"name"`
	// InputType is the data type of the input to the event or method
	Type string `json:"type"`
	// StartPos is the starting position from which to extract information from the hex data
	Indexed bool `json:"indexed"`
}

type AbiEvent struct {
	// ContractAddress is the contract address that the event belongs to
	ContractAddress string
	// Name is the name of the event
	Name string
	// Inputs is the slice of AbiContractColumn which are the inputs of the event
	Inputs []AbiContractColumn
	// InputsJson is the json string of inputs data
	InputsJson string
	// SigHash is the hash of the event signature
	SigHash string
	// Namespace is the namespace prefix added to the name of the SQL view
	Namespace string
}

type AbiMethod struct {
	// ContractAddress is the contract address that the method belongs to
	ContractAddress string
	// Name is the name of the method
	Name string
	// Inputs is the slice of AbiContractColumn which contains the inputs of the method
	Inputs []AbiContractColumn
	// InputsJson is the json string of inputs data
	InputsJson string
	// MethodIdHash is the hash of the method ID
	MethodIdHash string
	// Namespace is the namespace prefix added to the name of the SQL view
	Namespace string
}

type Options struct {
	DSN string
	Namespace string
	DryRun bool
	Drop bool
	Limit int
	Count int
}

func NewOptions(dsn, namespace string, dryRun, drop bool, limit, count int) *Options {
	return &Options{
		DSN: dsn,
		Namespace: namespace,
		DryRun: dryRun,
		Drop: drop,
		Limit: limit,
		Count: count,
	}
}

func NewAbiContract(contractAddress string, abi abi.ABI, namespace string) *AbiContract {
	return &AbiContract{
		Events:  newAbiEvents(abi, contractAddress, namespace),
		Methods: newAbiMethods(abi, contractAddress, namespace),
		Skip:    false,
	}
}

func newAbiEvent(event abi.Event, contractAddress string, namespace string) *AbiEvent {
	return &AbiEvent{
		ContractAddress: contractAddress,
		Name:            event.Name,
		SigHash:         event.ID.Hex(),
		Inputs:          createInputs(event.Inputs),
		InputsJson:      inputsToJson(createInputs(event.Inputs)),
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
		Inputs:          createInputs(method.Inputs),
		InputsJson:      inputsToJson(createInputs(method.Inputs)),
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

func inputsToJson(v []AbiContractColumn) string {

	bs, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}

	return string(bs)
}

func createInputs(inputs abi.Arguments) []AbiContractColumn {
	newInputs := make([]AbiContractColumn, len(inputs))
	for idx, input := range inputs {
		newInputs[idx] = createInput(input, idx)
	}

	return newInputs
}

func createInput(input abi.Argument, idx int) AbiContractColumn {
	return AbiContractColumn{
		Name:    validateInputName(input.Name, idx),
		Type:    input.Type.String(),
		Indexed: input.Indexed,
	}
}

func getMethodIdHash(methodId []byte) string {
	return fmt.Sprintf("0x%s", hex.EncodeToString(methodId))
}

func validateInputName(input string, idx int) string {
	if input == "" {
		return fmt.Sprintf("inp_%s", strconv.Itoa(idx))
	}

	return input
}

func (c *AbiContract) GenerateSql() bytes.Buffer {
	buffer := bytes.Buffer{}
	for _, v := range c.Events {
		_, err := buffer.Write(v.generateSql())
		if err != nil {
			log.Fatal(err)
		}
	}

	for _, v := range c.Methods {
		_, err := buffer.Write(v.generateSql())
		if err != nil {
			log.Fatal(err)
		}
	}

	return buffer
}

func (e *AbiEvent) generateSql() []byte {
	fpath, err := filepath.Abs("templates/event.sql")
	if err != nil {
		log.Fatal(err)
	}

	t, err := template.New("event.sql").ParseFiles(fpath)
	if err != nil {
		log.Fatal(err)
	}

	buffer := bytes.Buffer{}
	err = t.Execute(&buffer, e)
	if err != nil {
		log.Fatal(err)
	}

	return buffer.Bytes()
}

func (m *AbiMethod) generateSql() []byte {
	fpath, err := filepath.Abs("templates/function.sql")
	if err != nil {
		log.Fatal(err)
	}

	t, err := template.New("function.sql").ParseFiles(fpath)
	if err != nil {
		log.Fatal(err)
	}

	if err != nil {
		log.Fatal(err)
	}

	buffer := bytes.Buffer{}
	err = t.Execute(&buffer, m)
	if err != nil {
		log.Fatal(err)
	}

	return buffer.Bytes()
}

func (e *AbiEvent) isValidName() bool {
	// Constant 6 represents underscores + 'evt' in table name
	tableNameLength := len(e.Namespace) + len(e.ContractAddress) + len(e.Name) + 6

	return tableNameLength <= snowflakeIdentifierMaxLength
}

func (m *AbiMethod) isValidName() bool {
	// Constant 5 represents underscores + 'fn' in table name
	tableNameLength := len(m.Namespace) + len(m.ContractAddress) + len(m.Name) + 5

	return tableNameLength <= snowflakeIdentifierMaxLength
}

func (c *AbiContract) GetNumberOfStatements() int {
	return len(c.Events) + len(c.Methods)
}

func (c *AbiContract) ValidateNames() {
	// if any event or function names are not valid set skip to true and exit immediately
	for _, e := range c.Events {
		if !e.isValidName() {
			c.Skip = true
			log.Println("event name too long:", e.Name)
			return
		}
	}

	for _, m := range c.Methods {
		if !m.isValidName() {
			c.Skip = true
			log.Println("method name too long:", m.Name)
			return
		}
	}
}
