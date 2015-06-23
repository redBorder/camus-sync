package net.redborder.camus;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.*;

public class OptionsWithFile {
    public static Logger log = LoggerFactory.getLogger(OptionsWithFile.class);

    private Options options;
    private CommandLine cmdLine;
    private CommandLineParser parser;
    private HelpFormatter helpFormatter;
    private String[] args;
    private String configFileOpt;
    private ConfigFile configFile;

    public OptionsWithFile(String[] args, String configFileOpt) {
        this.cmdLine = null;
        this.options = new Options();
        this.parser = new BasicParser();
        this.helpFormatter = new HelpFormatter();
        this.configFileOpt = configFileOpt;
        this.args = args;
    }

    public void build() {
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("One or more of the options specified are not allowed. Check the options and try again.");
            System.exit(1);
        }

        if (!cmdLine.hasOption(configFileOpt)) {
            log.error("Config file option not present! You must specify it with the option -{} CONFIG_FILE_PATH", configFileOpt);
            System.exit(1);
        } else {
            try {
                configFile = new ConfigFile(cmdLine.getOptionValue(configFileOpt));
            } catch (FileNotFoundException e) {
                log.error("Config file couldn't be found! Specify a valid path");
                System.exit(1);
            }
        }
    }

    public void addOption(String shorter, String longer, boolean hasParam, String description) {
        options.addOption(shorter, longer, hasParam, description);
    }

    public String getOption(String optionName) {
        String optionValue = cmdLine.getOptionValue(optionName);

        if (optionValue == null) {
            optionValue = String.valueOf(configFile.get(optionName));
        }

        return optionValue;
    }

    public boolean hasOption(String optionName) {
        boolean hasOption = cmdLine.hasOption(optionName);

        if (!hasOption) {
            hasOption = (configFile.get(optionName) != null);
        }

        return hasOption;
    }

    public List<String> getListOption(String optionName) {
        String optionValueCmd = cmdLine.getOptionValue(optionName);
        List<String> optionValue = (List<String>) configFile.get(optionName);

        if (optionValueCmd != null) {
            String[] tokenized = optionValueCmd.split(",");
            optionValue = Arrays.asList(tokenized);
        }

        return optionValue;
    }

    public List<String> getListKeysOption(String optionName) {
        String optionValue = cmdLine.getOptionValue(optionName);
        List<String> returned;

        if (optionValue == null) {
            Map<String, Object> mapValues = (Map<String, Object>) configFile.get(optionName);
            Set<String> mapKeySet = mapValues.keySet();
            returned = new ArrayList<>(mapKeySet);
        } else {
            String[] tokenized = optionValue.split(",");
            returned = Arrays.asList(tokenized);
        }

        return returned;
    }

    public ConfigFile getConfig() {
        return configFile;
    }

    public void printHelp(String usageLine) {
        helpFormatter.printHelp(usageLine, options);
    }
}
