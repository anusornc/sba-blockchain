package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/hyperledger/fabric-chaincode-go/v2/shim"
	"github.com/hyperledger/fabric-contract-api-go/v2/contractapi"
)

const (
	recordKeyPrefix   = "entity:"
	recallKeyPrefix   = "recall:"
	activityKeyPrefix = "activity:"
	firmLabelPrefix   = "firm-label:"
	firmIndexName     = "firm~entity"
)

type OpenFDAContract struct {
	contractapi.Contract
}

type serverConfig struct {
	CCID    string
	Address string
}

type OpenFDARecord struct {
	RecallNumber        string `json:"recall_number"`
	EventID             string `json:"event_id"`
	RecallingFirm       string `json:"recalling_firm"`
	ProductDescription  string `json:"product_description"`
	City                string `json:"city"`
	State               string `json:"state"`
	Country             string `json:"country"`
	DistributionPattern string `json:"distribution_pattern"`
	EntityID            string `json:"entity_id"`
	ActivityID          string `json:"activity_id"`
	AgentID             string `json:"agent_id"`
}

func (r OpenFDARecord) Validate() error {
	if r.RecallNumber == "" {
		return fmt.Errorf("recall_number is required")
	}
	if r.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if r.RecallingFirm == "" {
		return fmt.Errorf("recalling_firm is required")
	}
	if r.ProductDescription == "" {
		return fmt.Errorf("product_description is required")
	}
	if r.EntityID == "" {
		return fmt.Errorf("entity_id is required")
	}
	if r.ActivityID == "" {
		return fmt.Errorf("activity_id is required")
	}
	return nil
}

func firmHash(firm string) string {
	sum := sha256.Sum256([]byte(firm))
	return hex.EncodeToString(sum[:])
}

func recordKey(entityID string) string {
	return recordKeyPrefix + entityID
}

func (s *OpenFDAContract) PutRecord(ctx contractapi.TransactionContextInterface, recordJSON string) error {
	var record OpenFDARecord
	if err := json.Unmarshal([]byte(recordJSON), &record); err != nil {
		return fmt.Errorf("invalid openFDA record JSON: %w", err)
	}
	if err := record.Validate(); err != nil {
		return err
	}

	canonical, err := json.Marshal(record)
	if err != nil {
		return err
	}

	stub := ctx.GetStub()
	if err := stub.PutState(recordKey(record.EntityID), canonical); err != nil {
		return fmt.Errorf("put entity record: %w", err)
	}
	if err := stub.PutState(recallKeyPrefix+record.RecallNumber, []byte(record.EntityID)); err != nil {
		return fmt.Errorf("put recall index: %w", err)
	}
	if err := stub.PutState(activityKeyPrefix+record.ActivityID, []byte(record.EntityID)); err != nil {
		return fmt.Errorf("put activity index: %w", err)
	}

	firmID := firmHash(record.RecallingFirm)
	firmEntityKey, err := stub.CreateCompositeKey(firmIndexName, []string{firmID, record.EntityID})
	if err != nil {
		return err
	}
	if err := stub.PutState(firmEntityKey, []byte{1}); err != nil {
		return fmt.Errorf("put firm index: %w", err)
	}
	return stub.PutState(firmLabelPrefix+firmID, []byte(record.RecallingFirm))
}

func (s *OpenFDAContract) GetByRecallNumber(ctx contractapi.TransactionContextInterface, recallNumber string) (*OpenFDARecord, error) {
	entityID, err := ctx.GetStub().GetState(recallKeyPrefix + recallNumber)
	if err != nil {
		return nil, err
	}
	if entityID == nil {
		return nil, fmt.Errorf("recall_number not found: %s", recallNumber)
	}
	return s.GetByEntityID(ctx, string(entityID))
}

func (s *OpenFDAContract) GetByEntityID(ctx contractapi.TransactionContextInterface, entityID string) (*OpenFDARecord, error) {
	recordJSON, err := ctx.GetStub().GetState(recordKey(entityID))
	if err != nil {
		return nil, err
	}
	if recordJSON == nil {
		return nil, fmt.Errorf("entity_id not found: %s", entityID)
	}
	var record OpenFDARecord
	if err := json.Unmarshal(recordJSON, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (s *OpenFDAContract) GetByActivityID(ctx contractapi.TransactionContextInterface, activityID string) (*OpenFDARecord, error) {
	entityID, err := ctx.GetStub().GetState(activityKeyPrefix + activityID)
	if err != nil {
		return nil, err
	}
	if entityID == nil {
		return nil, fmt.Errorf("activity_id not found: %s", activityID)
	}
	return s.GetByEntityID(ctx, string(entityID))
}

func (s *OpenFDAContract) CountByFirm(ctx contractapi.TransactionContextInterface, recallingFirm string) (int, error) {
	iterator, err := ctx.GetStub().GetStateByPartialCompositeKey(firmIndexName, []string{firmHash(recallingFirm)})
	if err != nil {
		return 0, err
	}
	defer iterator.Close()

	count := 0
	for iterator.HasNext() {
		if _, err := iterator.Next(); err != nil {
			return 0, err
		}
		count++
	}
	return count, nil
}

func main() {
	chaincode, err := contractapi.NewChaincode(&OpenFDAContract{})
	if err != nil {
		log.Panicf("create openFDA chaincode: %v", err)
	}

	if address := os.Getenv("CHAINCODE_SERVER_ADDRESS"); address != "" {
		config := serverConfig{
			CCID:    os.Getenv("CHAINCODE_ID"),
			Address: address,
		}
		server := &shim.ChaincodeServer{
			CCID:     config.CCID,
			Address:  config.Address,
			CC:       chaincode,
			TLSProps: getTLSProperties(),
		}
		if err := server.Start(); err != nil {
			log.Panicf("start openFDA chaincode service: %v", err)
		}
		return
	}

	if err := chaincode.Start(); err != nil {
		log.Panicf("start openFDA chaincode: %v", err)
	}
}

func getTLSProperties() shim.TLSProperties {
	tlsDisabledStr := getEnvOrDefault("CHAINCODE_TLS_DISABLED", "true")
	key := getEnvOrDefault("CHAINCODE_TLS_KEY", "")
	cert := getEnvOrDefault("CHAINCODE_TLS_CERT", "")
	clientCACert := getEnvOrDefault("CHAINCODE_CLIENT_CA_CERT", "")

	tlsDisabled := getBoolOrDefault(tlsDisabledStr, false)
	var keyBytes, certBytes, clientCACertBytes []byte
	var err error
	if !tlsDisabled {
		keyBytes, err = os.ReadFile(key)
		if err != nil {
			log.Panicf("read chaincode TLS key: %v", err)
		}
		certBytes, err = os.ReadFile(cert)
		if err != nil {
			log.Panicf("read chaincode TLS cert: %v", err)
		}
	}
	if clientCACert != "" {
		clientCACertBytes, err = os.ReadFile(clientCACert)
		if err != nil {
			log.Panicf("read chaincode client CA cert: %v", err)
		}
	}
	return shim.TLSProperties{
		Disabled:      tlsDisabled,
		Key:           keyBytes,
		Cert:          certBytes,
		ClientCACerts: clientCACertBytes,
	}
}

func getEnvOrDefault(env, defaultVal string) string {
	value, ok := os.LookupEnv(env)
	if !ok {
		value = defaultVal
	}
	return value
}

func getBoolOrDefault(value string, defaultVal bool) bool {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return defaultVal
	}
	return parsed
}
