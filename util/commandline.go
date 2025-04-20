package util

import "errors"

func ParseCommandLine(commandline string) ([]string, error) {
	var args []string
	var currentArg []rune
	var inDoubleQuote bool
	var inSingleQuote bool
	var escaped bool

	for _, r := range commandline {
		if escaped {
			currentArg = append(currentArg, r)
			escaped = false
			continue
		}

		if r == '\\' {
			escaped = true
			continue
		}

		if r == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			continue
		}
		if r == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			continue
		}

		if (r == ' ' || r == '\t') && !inDoubleQuote && !inSingleQuote {
			if len(currentArg) > 0 {
				args = append(args, string(currentArg))
				currentArg = nil
			}
			continue
		}

		currentArg = append(currentArg, r)
	}

	if inDoubleQuote {
		return nil, errors.New("unclosed double quote in command line")
	}
	if inSingleQuote {
		return nil, errors.New("unclosed single quote in command line")
	}

	if len(currentArg) > 0 {
		args = append(args, string(currentArg))
	}

	return args, nil
}
