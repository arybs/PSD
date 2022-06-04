package psd
import java.sql.Timestamp

object utils {
  def toTimeStamp(StringTimeStamp: String) = Timestamp.valueOf(
    ("2022/"+StringTimeStamp).replace("-", " ").replace("/", "-"))

}
