package globesort;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.*;
import net.sourceforge.argparse4j.inf.*;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.lang.RuntimeException;
import java.lang.Exception;
import java.lang.System;
import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GlobeSortClient {

    private final ManagedChannel serverChannel;
    private final GlobeSortGrpc.GlobeSortBlockingStub serverStub;

	private static int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    private String serverStr;

    public GlobeSortClient(String ip, int port) {
        this.serverChannel = ManagedChannelBuilder.forAddress(ip, port)
				.maxInboundMessageSize(MAX_MESSAGE_SIZE)
                .usePlaintext(true).build();
        this.serverStub = GlobeSortGrpc.newBlockingStub(serverChannel);

        this.serverStr = ip + ":" + port;
    }

    public void run(Integer[] values, int length) throws Exception {
        long startTime, total_time, total_time_ms, latency, two_way_time;
        double app_t, one_way_t;
        
        // Run ping
        System.out.println("Pinging " + serverStr + "...");
        startTime = System.nanoTime();
        serverStub.ping(Empty.newBuilder().build());
        latency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
        System.out.println("Ping successful: " + latency + "ms");

        System.out.println("Requesting server to sort array");
        IntArray request = IntArray.newBuilder().addAllValues(Arrays.asList(values)).build();

        // Sort array
        startTime = System.nanoTime();
        SortResponse response = serverStub.sortIntegers(request);
        total_time = System.nanoTime() - startTime;

        two_way_time = 
          TimeUnit.NANOSECONDS.toMillis(total_time - response.getNanoSeconds());
        one_way_t = (double)(length * 4.0)/(two_way_time / 500.0);

        total_time_ms = TimeUnit.NANOSECONDS.toMillis(total_time);
        if (total_time_ms == 0) {
          app_t = -1;
        }
        else {
          app_t =
          (double)(length/(total_time_ms / 1000.0));
        }

        System.out.println("Application throughput: " + app_t + " ints/sec");
        System.out.println("One-way throughput: " + one_way_t + " bytes/sec\n");
    }

    public void shutdown() throws InterruptedException {
        serverChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
    }

    private static Integer[] genValues(int numValues) {
        ArrayList<Integer> vals = new ArrayList<Integer>();
        Random randGen = new Random();
        for(int i : randGen.ints(numValues).toArray()){
            vals.add(i);
        }
        return vals.toArray(new Integer[vals.size()]);
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("GlobeSortClient").build()
                .description("GlobeSort client");
        parser.addArgument("server_ip").type(String.class)
                .help("Server IP address");
        parser.addArgument("server_port").type(Integer.class)
                .help("Server port");
        parser.addArgument("num_values").type(Integer.class)
                .help("Number of values to sort");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace cmd_args = parseArgs(args);
        if (cmd_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }

        Integer[] values = genValues(cmd_args.getInt("num_values"));

        GlobeSortClient client = new GlobeSortClient(cmd_args.getString("server_ip"), cmd_args.getInt("server_port"));
        try {
            client.run(values, cmd_args.getInt("num_values"));
        } finally {
            client.shutdown();
        }
    }
}
