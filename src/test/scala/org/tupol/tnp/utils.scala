package org.tupol.tnp

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{ Path, StandardOpenOption }

import org.tupol.utils.Bracket

object utils {

  def writeStringToFile(path: Path, string: String): Path = {
    Bracket
      .auto(FileChannel.open(path, StandardOpenOption.WRITE)) { file =>
        file.write(ByteBuffer.wrap(string.getBytes))
      }.get
    path
  }
}
