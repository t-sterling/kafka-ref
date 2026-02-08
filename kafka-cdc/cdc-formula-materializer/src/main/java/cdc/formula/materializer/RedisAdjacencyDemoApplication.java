package cdc.formula.materializer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Set;

@SpringBootApplication
public class RedisAdjacencyDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisAdjacencyDemoApplication.class, args);
    }

    @Bean
    CommandLineRunner demo(AdjacencyListRepository repo) {
        return args -> {
            String parent = "account:123";

            // (optional) start clean for demo
            repo.deleteList(parent);

            // c) add items
            repo.addEdge(parent, "child:A");
            repo.addEdge(parent, "child:B");
            repo.addEdge(parent, "child:C");

            // b) adjacency list read
            Set<String> neighbors = repo.getNeighbors(parent);
            System.out.println("Neighbors after adds: " + neighbors);

            // c) remove item
            repo.removeEdge(parent, "child:B");
            System.out.println("Neighbors after remove: " + repo.getNeighbors(parent));

            // idempotent add (SET semantics)
            repo.addEdge(parent, "child:A");
            System.out.println("Neighbors after re-add A: " + repo.getNeighbors(parent));
        };
    }
}
