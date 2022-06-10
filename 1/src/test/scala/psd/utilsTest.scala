package psd

import org.scalatest.flatspec.AnyFlatSpec

class utilsTest extends AnyFlatSpec {
  it should ("should Return long from timestamp string") in{
    val toWork = "06/03-03:46:46.152324"
    assert(utils.toTimeStamp(toWork).getTime == java.sql.Timestamp.valueOf("2022-06-03 03:46:46.15232").getTime)
  }

}
