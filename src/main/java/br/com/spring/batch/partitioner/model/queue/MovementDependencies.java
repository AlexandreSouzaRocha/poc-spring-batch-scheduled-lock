package br.com.spring.batch.partitioner.model.queue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import br.com.spring.batch.partitioner.model.enums.MovementType;

public record MovementDependencies(Map<MovementType, Set<MovementType>> requirements) {

    public MovementDependencies {
        requirements = Map.copyOf(requirements);
        requireNoCycle(requirements);
    }

    public static MovementDependencies none() {
        return new MovementDependencies(Map.of());
    }

    public static MovementDependencies of(Map<MovementType, List<MovementType>> configured) {
        return new MovementDependencies(configured.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Set.copyOf(entry.getValue()))));
    }

    public Set<MovementType> requiredBy(MovementType type) {
        return requirements.getOrDefault(type, Set.of());
    }

    public boolean isReleased(MovementType type, Set<MovementType> pendingTypes) {
        return requiredBy(type).stream().noneMatch(pendingTypes::contains);
    }

    private static void requireNoCycle(Map<MovementType, Set<MovementType>> requirements) {
        requirements.keySet().forEach(type -> requireReachableWithoutCycle(type, requirements));
    }

    private static void requireReachableWithoutCycle(MovementType start,
            Map<MovementType, Set<MovementType>> requirements) {
        Deque<MovementType> pending = new ArrayDeque<>(requirements.getOrDefault(start, Set.of()));
        Set<MovementType> seen = new java.util.HashSet<>();
        while (!pending.isEmpty()) {
            MovementType current = pending.pop();
            if (current == start) {
                throw new IllegalStateException("dependência cíclica entre tipos de movimento envolvendo " + start);
            }
            if (seen.add(current)) {
                pending.addAll(requirements.getOrDefault(current, Set.of()));
            }
        }
    }
}
