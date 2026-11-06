package io.aperiodic.tparquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Every payload vector of {@code testdata/payloads/manifest.json} (testdata/README.md). */
class PayloadVectorsTest {
  static Stream<Testdata.Payload> valid() {
    return Testdata.payloads().stream().filter(Testdata.Payload::valid);
  }

  static Stream<Testdata.Payload> invalid() {
    return Testdata.payloads().stream().filter(p -> !p.valid());
  }

  @Test
  void theManifestHasBothKinds() {
    assertThat(valid().count()).isGreaterThanOrEqualTo(10);
    assertThat(invalid().count()).isGreaterThanOrEqualTo(10);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("valid")
  void decodesExactly(Testdata.Payload v) throws Exception {
    assertThat(Pcodec.decompress(v.payload(), v.expectedLen())).isEqualTo(v.decoded());
    assertThat(Pcodec.mode(v.payload()).name()).isEqualTo(v.mode());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("valid")
  void decodesExactlyFromDirectBuffers(Testdata.Payload v) throws Exception {
    ByteBuffer src = ByteBuffer.allocateDirect(v.payload().length).put(v.payload()).flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(v.expectedLen());
    Pcodec.decompress(src, dst, v.expectedLen());
    byte[] got = new byte[v.expectedLen()];
    dst.flip().get(got);
    assertThat(got).isEqualTo(v.decoded());
  }

  /** The vectors were written with decimal on; the same arguments give the same bytes. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("valid")
  void reEncodingGivesTheSamePayload(Testdata.Payload v) {
    byte[] d = v.decoded();
    assertThat(Pcodec.compress(d, 0, d.length, v.hint(), v.level(), true)).isEqualTo(v.payload());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("invalid")
  void refuses(Testdata.Payload v) {
    assertThatThrownBy(() -> Pcodec.decompress(v.payload(), v.expectedLen()))
        .isInstanceOf(PcodecException.class)
        .hasMessageStartingWith("pcodec payload: ");
    ByteBuffer src = ByteBuffer.allocateDirect(Math.max(1, v.payload().length)).put(v.payload()).flip();
    ByteBuffer dst = ByteBuffer.allocateDirect(v.expectedLen());
    assertThatThrownBy(() -> Pcodec.decompress(src, dst, v.expectedLen())).isInstanceOf(PcodecException.class);
    assertThat(src.position()).isZero();
    assertThat(dst.position()).isZero();
  }

  /** The reference decoder's message (informative in the manifest; this binding passes it through). */
  @ParameterizedTest(name = "{0}")
  @MethodSource("invalid")
  void refusesWithTheReferenceMessage(Testdata.Payload v) {
    assertThatThrownBy(() -> Pcodec.decompress(v.payload(), v.expectedLen())).hasMessage(v.error());
  }
}
