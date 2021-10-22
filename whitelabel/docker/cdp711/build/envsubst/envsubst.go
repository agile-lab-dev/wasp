package main

import (
    "flag"
    "io/ioutil"
    "log"
    "regexp"
    "strings"
)

func main() {
    // Parse flags
    environmentDirectory := flag.String("env", "/", "Directory containing files with environment variables")
    substitutionDirectory := flag.String("sub", "/", "Directory containing files where variables will be substituted")
    outputDirectory := flag.String("out", "/", "Output directory if the script doesn't work in-place")
    processAnyFile := flag.Bool("any", false, "If set to false only .env and .variables files will be processed")
    flag.Parse()
    if !strings.HasSuffix(*environmentDirectory, "/") {
        *environmentDirectory = *environmentDirectory + "/"
    }
    if !strings.HasSuffix(*substitutionDirectory, "/") {
        *substitutionDirectory = *substitutionDirectory + "/"
    }
    if *outputDirectory == "" {
        outputDirectory = substitutionDirectory
    }
    if !strings.HasSuffix(*outputDirectory, "/") {
        *outputDirectory = *outputDirectory + "/"
    }

    log.Println("Looking for environment variables in " + *environmentDirectory)
    log.Println("Looking for variables to replace in " + *substitutionDirectory)
    log.Println("Writing output to " + *outputDirectory)
    log.Println("_____________________________________")
    log.Println("")

    environmentVariables := make(map[string]string)

    // Read environment files
    environmentFiles, e := ioutil.ReadDir(*environmentDirectory)
    if e != nil {
        log.Fatal(e)
    }
    for _, environmentFile := range environmentFiles {
        if !*processAnyFile && !strings.HasSuffix(environmentFile.Name(), ".env") {
            continue
        }

        log.Println("Reading " + *environmentDirectory + environmentFile.Name())
        file, e := ioutil.ReadFile(*environmentDirectory + environmentFile.Name())
        if e != nil {
            log.Fatal(e)
        }
        lines := strings.Split(string(file), "\n")
        for _, line := range lines {
            keyValue := strings.Split(line, "=")
            if len(keyValue) == 2 {
                environmentVariables[keyValue[0]] = keyValue[1]
            } else {
                log.Println("Not reading line: " + line)
            }
        }
    }

    // Replace variables in files
    substitionFiles, e := ioutil.ReadDir(*substitutionDirectory)
    if e != nil {
        log.Fatal(e)
    }
    environmentRegex := regexp.MustCompile("\\$\\{(.*?)\\}")
    for _, substitionFile := range substitionFiles {
        if !*processAnyFile && !strings.HasSuffix(substitionFile.Name(), ".variable") {
            continue
        }

        log.Println("Reading " + *substitutionDirectory + substitionFile.Name())
        file, e := ioutil.ReadFile(*substitutionDirectory + substitionFile.Name())
        if e != nil {
            log.Fatal(e)
        }
        lines := strings.Split(string(file), "\n")
        for i, line := range lines {
            matches := environmentRegex.FindAllStringSubmatch(line, -1)
            for _, match := range matches {
                variable := "${" + match[1] + "}"
                lines[i] = strings.ReplaceAll(line, variable, environmentVariables[match[1]])
            }
        }
        output := strings.Join(lines, "\n")
        var outputFile string
        if *processAnyFile {
            outputFile = *outputDirectory + substitionFile.Name()
        } else {
            outputFile = *outputDirectory + strings.Replace(substitionFile.Name(), ".variable", "", -1)
        }
        if e := ioutil.WriteFile(outputFile, []byte(output), 0644); e != nil {
            log.Fatal(e)
        }
        log.Println("Writing " + outputFile)
    }

}