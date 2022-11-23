package notify

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type generic struct {
	url string
}

func genericSender(url string) *slack {
	return &slack{
		url: url,
	}
}

func (s *generic) send(subject, content string) error {
	resp, err := http.Post(
		s.url,
		"application/json",
		bytes.NewBuffer([]byte(fmt.Sprintf("{\"subject\": \"%s\", \"content\": \"%s\"}", subject, content))),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack webhook returns an unexpect response: %s", string(body))
	}

	return nil
}
