package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	src "github.com/zograf/WalGOritam/src"
)

func help() {
	fmt.Println("Usage: PUT <key> <value>")
	fmt.Println("       GET <key>")
	fmt.Println("       DEL <key>")
	fmt.Println("")
	fmt.Println("Quit:  EXIT")
}

func main() {
	src.TestCache()
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.ReplaceAll(text, "\n", "")
		if text == "EXIT" {
			os.Exit(0)
		}
		tokens := strings.Split(text, " ")
		if tokens[0] == "PUT" {
			if len(tokens) == 3 {
				src.EnginePut(tokens[1], tokens[2])
				continue
			}
		} else if tokens[0] == "GET" {
			if len(tokens) == 2 {
				src.EngineGet(tokens[1])
				continue
			}
		} else if tokens[0] == "DEL" {
			if len(tokens) == 2 {
				src.EngineDelete(tokens[1])
				continue
			}
		}
		help()
	}
}
