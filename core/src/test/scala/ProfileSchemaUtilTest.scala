import com.access.core.ProfileSchemaUtil
import org.junit.Test

class ProfileSchemaUtilTest {

  @Test
  def test(): Unit ={

    val structType = ProfileSchemaUtil.getScheme("user&category")
    println(structType)

  }
}
