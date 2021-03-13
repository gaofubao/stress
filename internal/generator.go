package internal

import (
	"bytes"
	"encoding/json"
	"log"

	"github.com/gaofubao/stress/v1.0.0/buffer"

	"github.com/bxcodec/faker/v3"
)

// 数据生成
type Generator interface {
	Generate(pool buffer.Pool)
}

// 从Faker中生成数据
type genFromFaker struct {
}

type FakerTypeOne struct {
	CreditCardNumber   string  `faker:"cc_number"`
	CreditCardType     string  `faker:"cc_type"`
	Email              string  `faker:"email"`
	DomainName         string  `faker:"domain_name"`
	IPV4               string  `faker:"ipv4"`
	PhoneNumber        string  `faker:"phone_number"`
	MacAddress         string  `faker:"mac_address"`
	URL                string  `faker:"url"`
	UserName           string  `faker:"username"`
	E164PhoneNumber    string  `faker:"e_164_phone_number"`
	Name               string  `faker:"name"`
	Timestamp          string  `faker:"timestamp"`
	Century            string  `faker:"century"`
	Sentence           string  `faker:"sentence"`
	Paragraph          string  `faker:"paragraph"`
	Currency           string  `faker:"currency"`
	Amount             float64 `faker:"amount"`
	AmountWithCurrency string  `faker:"amount_with_currency"`
	PaymentMethod      string  `faker:"oneof: cc, paypal, check, money order"`
}

func NewGenFromFaker() (Generator, error) {
	return &genFromFaker{}, nil
}

func (s *genFromFaker) Generate(pool buffer.Pool) {
	dataFormat := FakerTypeOne{}
	for {
		err := faker.FakeData(&dataFormat)
		if err != nil {
			log.Println("Fake data失败:", err.Error())
			continue
		}
		dataBytes, err := json.Marshal(dataFormat)
		if err != nil {
			log.Println("序列化数据失败:", err.Error())
			continue
		}
		buffer := bytes.NewBuffer(dataBytes)
		// 发送的消息以"\n"结尾，避免粘包
		buffer.Write([]byte("\n"))
		err = pool.Put(buffer.Bytes())
		if err != nil {
			log.Println("数据写入缓冲池失败", err.Error())
			continue
		}
	}
}

// TODO: 用户指定数据格式
