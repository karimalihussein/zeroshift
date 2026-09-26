package io.zeroshift.failures;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Files;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CoordinatorProcessTest {
  @TempDir File dir;

  @Test
  void aSpringBootJarIsLaunchedThroughItsLauncher() throws Exception {
    var boot = new File(dir, "app.jar");
    try (var jar = new JarOutputStream(Files.newOutputStream(boot.toPath()))) {
      jar.putNextEntry(new JarEntry("BOOT-INF/"));
      jar.closeEntry();
    }
    var plain = new File(dir, "plain.jar");
    try (var jar = new JarOutputStream(Files.newOutputStream(plain.toPath()))) {
      jar.putNextEntry(new JarEntry("io/"));
      jar.closeEntry();
    }
    assertThat(CoordinatorProcess.isBootJar(boot.getPath())).isTrue();
    assertThat(CoordinatorProcess.isBootJar(plain.getPath())).isFalse();
    assertThat(CoordinatorProcess.isBootJar("target/classes" + File.pathSeparator + boot.getPath()))
        .isFalse();
  }

  @Test
  void onlyTheLabsDatabasesCanBeNamed() {
    assertThat(FailureDb.check("fl_ledger_safe_backup")).isEqualTo("fl_ledger_safe_backup");
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> FailureDb.check("orders"))
        .isInstanceOf(IllegalArgumentException.class);
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> FailureDb.check("fl_x; DROP DATABASE orders"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
