package abi

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/ethereum/go-ethereum/crypto"
)

const (
	fnInitialLength        = 11
	indexedInputLength     = 70
	unindexedInputLength   = 3
	individualInputLength  = 64
	namespace              = "ETH"
)

type Input struct {
	Indexed      bool   `json:"indexed"`
	InternalType string `json:"internalType"`
	Name         string `json:"name"`
	Type         string `json:"type"`
	StartPos     int    `json:"start_pos"`
	ColumnName   string `json:"column_name"`
}

type Abi struct {
	StateMutability string  `json:"stateMutability"`
	Outputs         []Input `json:"outputs"`
	Inputs          []Input `json:"inputs"`
	Type            string  `json:"type"`
	Name            string  `json:"name"`
	Anonymous       bool    `json:"anonymous"`
	MethodId        string  `json:"method_id"`
	SigHash         string  `json:"sig_hash"`
	ContractAddress string  `json:"contract_address"`
}

func (a *Abi) generateParamTypesString() string {
	s := make([]string, len(a.Inputs))
	for i, v := range a.Inputs {
		s[i] = v.Type
	}

	return strings.Join(s[:], ",")
}

func NewAbi(abi Abi, contractAddress string) Abi {
	abi.ContractAddress = contractAddress
	abi.SigHash = abi.sigHash()
	abi.MethodId = abi.methodId()
	abi.Inputs = abi.formatInputs()
	return abi
}

func (a *Abi) sigHash() string {
	bs := []byte(fmt.Sprintf("%s(%s)", a.Name, a.generateParamTypesString()))

	return crypto.Keccak256Hash(bs).String()
}

func calculateEvtStartPos(idx int, indexed bool) int {
	if indexed {
		return indexedInputLength + (idx * (individualInputLength + 3))
	}
	return unindexedInputLength + (idx * individualInputLength)
}

func calculateFnStartPos(idx int) int {
	return fnInitialLength + (idx * individualInputLength)
}

func getInputName(inputName string, idx int) string {
	if len(inputName) > 0 {
		return inputName
	}

	return fmt.Sprint(idx)
}

func (a *Abi) formatInputs() []Input {
	newInputs := []Input{}
	for idx, input := range a.Inputs {
		switch a.Type {
		case "function":
			input.StartPos = calculateFnStartPos(idx)
			input.ColumnName = "input"
		case "event":
			input.StartPos = calculateEvtStartPos(idx, input.Indexed)
			if input.Indexed {
				input.ColumnName = "topics"
			} else {
				input.ColumnName = "data"
			}
		default:
			continue
		}
		input.Name = getInputName(input.Name, idx)
		newInputs = append(newInputs, input)
	}
	return newInputs
}

func (a *Abi) methodId() string {
	return a.sigHash()[:10]
}

func (a *Abi) CreateSQLFile() {
	outputPath, err := filepath.Abs("out")
	if err != nil {
		log.Fatal(err)
	}
	if a.Type == "function" {
		filename := fmt.Sprintf("%s_%s_fn_%s.sql", namespace, a.ContractAddress, a.Name)
		fpath, err := filepath.Abs("templates/function.sql")
		if err != nil {
			log.Fatal(err)
		}
		t, err := template.New("function.sql").Delims("[[", "]]").ParseFiles(fpath)
		if err != nil {
			log.Fatal(err)
		}

		fp := filepath.Join(outputPath, filename)
		f, err := os.Create(fp)
		if err != nil {
			log.Fatal(err)
		}

		err = t.Execute(f, a)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		filename := fmt.Sprintf("%s_%s_evt_%s.sql", namespace, a.ContractAddress, a.Name)
		fpath, err := filepath.Abs("templates/event.sql")
		if err != nil {
			log.Fatal(err)
		}
		t, err := template.New("event.sql").Delims("[[", "]]").ParseFiles(fpath)
		if err != nil {
			log.Fatal(err)
		}

		fp := filepath.Join(outputPath, filename)
		f, err := os.Create(fp)
		if err != nil {
			log.Fatal(err)
		}

		err = t.Execute(f, a)
		if err != nil {
			log.Fatal(err)
		}
	}
}
