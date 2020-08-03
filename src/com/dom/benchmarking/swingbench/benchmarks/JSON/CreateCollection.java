package com.dom.benchmarking.swingbench.benchmarks.JSON;

import com.dom.util.OracleUtilities;

import java.sql.Connection;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.LogManager;

import oracle.soda.OracleCollection;
import oracle.soda.OracleDatabase;
import oracle.soda.OracleDocument;
import oracle.soda.rdbms.OracleRDBMSClient;
import oracle.soda.rdbms.OracleRDBMSMetadataBuilder;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class CreateCollection {

    public enum CommandLineOptions {
        USERNAME,
        PASSWORD,
        CONNECTSTRING
    }

    public enum Operation {
        DROP_COLLECTION,
        CREATE_COLLECTION,
    }

    public enum JSONParsing {
        LAX,
        STRICT,
        STANDARD
    }

    public enum JSONStorage {
        VARCHAR,
        BLOB,
        CLOB
    }

    public enum JSONCompression {
        NONE,
        LOW,
        MEDIUM,
        HIGH
    }

    public enum JSONVersioning {
        NONE,
        UUID,
        TIMESTAMP,
        MD5,
        SHA256,
        SEQUENTIAL
    }

    Operation operation = Operation.CREATE_COLLECTION;
    JSONParsing parsing = JSONParsing.STANDARD;
    JSONCompression compression = JSONCompression.NONE;
    JSONStorage storage = JSONStorage.BLOB.VARCHAR;
    JSONVersioning versioning = JSONVersioning.SHA256;

    public CreateCollection(String[] args) throws Exception {
        super();
        Map<CommandLineOptions, String> commandLineOptions = parseCommandLine(args);
        Connection connection =
                OracleUtilities.getConnection(commandLineOptions.get(CommandLineOptions.USERNAME), commandLineOptions.get(CommandLineOptions.PASSWORD), "jdbc:oracle:thin:@" +
                        commandLineOptions.get(CommandLineOptions.CONNECTSTRING));
        System.out.println("Got Connection");
        OracleRDBMSClient client = new OracleRDBMSClient();
        OracleDatabase database = client.getDatabase(connection);
        if (operation == Operation.CREATE_COLLECTION) {
            Statement s = connection.createStatement();
            s.execute("create sequence PASSENGER_SEQ");
            OracleRDBMSMetadataBuilder metabuilder = client.createMetadataBuilder();
            metabuilder.keyColumnType("NUMBER");
            metabuilder.keyColumnAssignmentMethod("SEQUENCE");
            metabuilder.keyColumnSequenceName("PASSENGER_SEQ");
            metabuilder.contentColumnName("PASSENGER_INFO");
            metabuilder.contentColumnType(storage.toString());
            metabuilder.contentColumnCompress(compression.toString());
            metabuilder.contentColumnValidation(parsing.toString());
            OracleDocument meta = metabuilder.build();
            //Create a collection if it doesn't exist
            OracleCollection collection = database.admin().createCollection("PASSENGERCOLLECTION", meta);
            System.out.println("Collection Created");
        } else if (operation == Operation.DROP_COLLECTION) {
            OracleCollection c = database.openCollection("PASSENGERCOLLECTION");
            c.admin().drop();
            System.out.println("Collection Dropped");
            Statement s = connection.createStatement();
            s.execute("drop sequence PASSENGER_SEQ");
        }
    }

    public static void main(String args[]) {
        try {
            CreateCollection cc = new CreateCollection(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Map<CommandLineOptions, String> parseCommandLine(String[] args) throws Exception {
        Options options = getOptions();
        CommandLineParser clp = new BasicParser();
        Map commandLineParameters = new HashMap();
        CommandLine cl = null;
        try {
            cl = clp.parse(options, args);

            if (cl.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("parameters:", options);
                System.exit(0);
            } else {


                if (cl.hasOption("u")) {
                    commandLineParameters.put(CommandLineOptions.USERNAME, cl.getOptionValue("u"));
                }
                if (cl.hasOption("p")) {
                    commandLineParameters.put(CommandLineOptions.PASSWORD, cl.getOptionValue("p"));
                }
                if (cl.hasOption("cs")) {
                    commandLineParameters.put(CommandLineOptions.CONNECTSTRING, cl.getOptionValue("cs"));
                }
                if (cl.hasOption("parsing")) {
                    parsing = JSONParsing.valueOf(cl.getOptionValue("parsing"));
                }
                if (cl.hasOption("compression")) {
                    compression = JSONCompression.valueOf(cl.getOptionValue("compression"));
                }
                if (cl.hasOption("storage")) {
                    storage = JSONStorage.valueOf(cl.getOptionValue("storage"));
                }
                if (cl.hasOption("drop")) {
                    operation = Operation.DROP_COLLECTION;
                }
                if (cl.hasOption("create")) {
                    operation = Operation.CREATE_COLLECTION;
                }
                if (cl.hasOption("debug")) {
                    System.setProperty("java.util.logging.config.class", "com.dom.util.logging.LoggerConfig");
                    LogManager.getLogManager().readConfiguration();
                }
            }
            return commandLineParameters;
        } catch (ParseException e) {
            System.out.println("ERROR : " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("parameters:", options);
            throw new RuntimeException(e.getMessage());
        }
    }


    public static Options getOptions() {
        Options options = new Options();
        Option option5 = new Option("u", "username for connections (requires access to V$ tables i.e. system)");
        option5.setArgs(1);
        option5.setRequired(true);
        Option option6 = new Option("p", "password of user");
        option6.setArgs(1);
        option6.setRequired(true);
        Option option7 = new Option("cs", "connect string of database you wish to monitor. i.e. //<servername>/<servicename>");
        option7.setArgs(1);
        option7.setRequired(true);
        OptionGroup og1 = new OptionGroup();
        Option option8 = new Option("drop", "drop collection");
        Option option9 = new Option("create", "create collection");
        og1.addOption(option8);
        og1.addOption(option9);
        og1.setRequired(true);
        Option option10 = new Option("parsing", "valid values are STANDARD|LAX|STRICT");
        option10.setArgs(1);
        option10.setRequired(false);
        Option option11 = new Option("compression", "valid values are NONE|LOW|MEDIUM|HIGH");
        option11.setArgs(1);
        option11.setRequired(false);
        Option option12 = new Option("storage", "valid values are VARCHAR|BLOB|CLOB");
        option12.setArgs(1);
        option12.setRequired(false);

        Option option100 = new Option("h", "print this message");
        option100.setLongOpt("help");
        Option option20 = new Option("debug", "turn on debug information");
        options.addOption(option5);
        options.addOption(option6);
        options.addOption(option7);
        options.addOption(option10);
        options.addOption(option11);
        options.addOption(option12);
        options.addOptionGroup(og1);
        options.addOption(option20);
        options.addOption(option100);
        return options;
    }
}
