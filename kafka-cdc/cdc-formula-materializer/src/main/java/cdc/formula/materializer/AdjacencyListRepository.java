package cdc.formula.materializer;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class AdjacencyListRepository {

    private final StringRedisTemplate redis;

    public AdjacencyListRepository(StringRedisTemplate redis) {
        this.redis = redis;
    }

    private static String key(String parentId) {
        // One Redis SET per parent node
        return "adj:" + parentId;
    }

    // b) create/read adjacency list (SET members)
    public Set<String> getNeighbors(String parentId) {
        return redis.opsForSet().members(key(parentId));
    }

    // c) add an edge: parent -> child
    public boolean addEdge(String parentId, String childId) {
        // SADD is atomic; returns number of new elements added (0 or 1 here)
        Long added = redis.opsForSet().add(key(parentId), childId);
        return added != null && added > 0;
    }

    // c) remove an edge: parent -X-> child
    public boolean removeEdge(String parentId, String childId) {
        // SREM is atomic; returns number removed (0 or 1)
        Long removed = redis.opsForSet().remove(key(parentId), childId);
        return removed != null && removed > 0;
    }

    public void deleteList(String parentId) {
        redis.delete(key(parentId));
    }
}
