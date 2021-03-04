package config

import "strings"

type CommaDelimitedString []string

func (s *CommaDelimitedString) DecodeString(value string) error {
	*s = strings.Split(value, ",")

	return nil
}
