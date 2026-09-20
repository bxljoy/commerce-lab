package com.commercelab.order;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

class CiStructureTest {
    private final Path root = Path.of(System.getProperty("basedir", ".")).toAbsolutePath().getParent();

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> steps() throws Exception {
        try (var input = Files.newInputStream(root.resolve(".github/workflows/ci.yml"))) {
            Map<String, Object> workflow = new Yaml().load(input);
            var jobs = (Map<String, Object>) workflow.get("jobs");
            var test = (Map<String, Object>) jobs.get("test");
            return (List<Map<String, Object>>) test.get("steps");
        }
    }

    @Test void normalImageGatesIncludeOutboxRestartAndInventory() throws Exception {
        var runs = steps().stream().map(step -> step.getOrDefault("run", "").toString()).toList();
        assertThat(runs).contains("make verify-outbox-recovery", "make verify-restart", "make verify-inventory-image");
        assertThat(String.join("\n", runs)).doesNotContain("verify-sync-recovery");
    }

    @Test void asyncProofGatesFollowServiceAndHistoricalImageVerification() throws Exception {
        var runs = steps().stream().map(step -> step.getOrDefault("run", "").toString()).toList();
        String boundaries = "python3 -B scripts/fixtures/test-async-completion-proof.py";
        String image = "make verify-async-completion";
        assertThat(runs).contains(boundaries, image);
        for (String prerequisite : List.of("make verify-order 2>&1 | tee test-output.log",
                "make verify-inventory", "make verify-restart", "make verify-inventory-image",
                "make verify-outbox-recovery")) {
            assertThat(runs.indexOf(boundaries)).isGreaterThan(runs.indexOf(prerequisite));
        }
        assertThat(runs.indexOf(image)).isGreaterThan(runs.indexOf(boundaries));
    }

    @Test void deliberateFailureLoopCoversAllFourImagesAndChecksExit97() throws Exception {
        var cleanup = steps().stream().map(step -> step.getOrDefault("run", "").toString())
                .filter(run -> run.contains("VERIFY_FAIL_AFTER_START")).toList();
        assertThat(cleanup).hasSize(1);
        assertThat(cleanup.getFirst()).contains(
                "for script in verify-order-restart verify-inventory-service verify-outbox-recovery verify-async-completion; do",
                "status=0", "VERIFY_FAIL_AFTER_START=1 bash \"scripts/${script}.sh\" || status=$?",
                "test \"$status\" -eq 97").doesNotContain("verify-sync-recovery");
    }

    @Test void makeKeepsIndependentServiceGatesAndPublishesOutboxTarget() throws Exception {
        String make = Files.readString(root.resolve("Makefile"));
        assertThat(make).contains("verify: verify-order verify-inventory", "verify-outbox-recovery:",
                "\tbash scripts/verify-outbox-recovery.sh",
                "\tmvn -f order-service/pom.xml -B verify", "\tmvn -f inventory-service/pom.xml -B verify")
                .doesNotContain("verify-sync-recovery");
    }

    @Test void startupAgentGuardAndProofHelperTestsRemainActive() throws Exception {
        String runs = String.join("\n", steps().stream()
                .map(step -> step.getOrDefault("run", "").toString()).toList());
        assertThat(runs).contains("make verify-order 2>&1 | tee test-output.log",
                "A Java agent has been loaded dynamically|Mockito is currently self-attaching",
                "exit 1", "make verify-inventory", "python3 -B scripts/fixtures/test-outbox-proof.py");
    }
}
