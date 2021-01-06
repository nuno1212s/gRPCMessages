package pt.nunogneto.trabalho.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BrokerServer {

    private static final int PORT = 5051;

    private static final Logger logger = Logger.getLogger(BrokerServer.class.getName());

    private Server server;

    private BrokerServerImpl implementation;

    public void start(long timeInHours) throws IOException {
        implementation = new BrokerServerImpl(logger, TimeUnit.HOURS.toMillis(timeInHours));

        server = ServerBuilder.forPort(PORT)
                .addService(implementation)
                .build().start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            implementation.shutdown();

            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) {
        BrokerServer brokerServer = new BrokerServer();

        Options options = new Options();

        Option expirationTime = new Option("ex", "expirationtime", true,
                "The expiration time, in hours, for the messages that are stored. (Default 5 Hours)");

        expirationTime.setRequired(false);

        options.addOption(expirationTime);

        CommandLineParser cmdLineParser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = cmdLineParser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Broker", options);

            System.exit(1);
            return;
        }

        long timeInHours = 5;

        if (cmd.hasOption("ex")) {
            try {
                timeInHours = Long.parseLong(cmd.getOptionValue("ex", "5"));
            } catch (NumberFormatException e) {
                logger.log(Level.SEVERE, "Time in hours must be a number");

                System.exit(1);
                return;
            }
        }

        try {
            brokerServer.start(timeInHours);

            brokerServer.blockUntilShutdown();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}
